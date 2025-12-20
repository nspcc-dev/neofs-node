package fstree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// FSTree represents an object storage as a filesystem tree.
type FSTree struct {
	Info

	*compression.Config
	log    *zap.Logger
	Depth  uint64
	writer writer

	noSync   bool
	readOnly bool
	shardID  string

	combinedCountLimit    int
	combinedSizeLimit     int
	combinedSizeThreshold int
	combinedWriteInterval time.Duration
}

// Info groups the information about file storage.
type Info struct {
	// Permission bits of the root directory.
	Permissions fs.FileMode

	// Full path to the root directory.
	RootPath string
}

// writer is an internal FS writing interface.
type writer interface {
	writeData(oid.ID, string, []byte) error
	finalize() error
	writeBatch([]writeDataUnit) error
}

type writeDataUnit struct {
	id   oid.ID
	path string
	data []byte
}

const (
	// DirNameLen is how many bytes is used to group keys into directories.
	DirNameLen = 1 // in bytes
	// MaxDepth is maximum depth of nested directories. 58^8 is 128e12 of
	// directories, enough for a single FSTree.
	MaxDepth = 8

	// combinedPrefix is the prefix that Protobuf message can't start with,
	// it reads as "field number 15 of type 7", but there is no type 7 in
	// the system (and we usually don't have 15 fields). ZSTD magic is also
	// different.
	combinedPrefix = 0x7f

	// combinedLenSize is sizeof(uint32), length of a serialized 32-bit BE integer.
	combinedLenSize = 4

	// combinedIDOff is the offset from the start of the combined prefix to OID.
	combinedIDOff = 2

	// combinedLengthOff is the offset from the start of the combined prefix to object length.
	combinedLengthOff = combinedIDOff + oid.Size

	// combinedDataOff is the offset from the start of the combined prefix to object data.
	// It's also the length of the prefix in total.
	combinedDataOff = combinedLengthOff + combinedLenSize
)

var _ common.Storage = (*FSTree)(nil)

func New(opts ...Option) *FSTree {
	f := &FSTree{
		Info: Info{
			Permissions: 0700,
			RootPath:    "./",
		},
		Config: nil,
		Depth:  4,

		combinedCountLimit:    128,
		combinedSizeLimit:     8 * 1024 * 1024,
		combinedSizeThreshold: 128 * 1024,
		combinedWriteInterval: 10 * time.Millisecond,
		log:                   zap.NewNop(),
	}
	for i := range opts {
		opts[i](f)
	}
	f.writer = newGenericWriter(f.Permissions, f.noSync)

	return f
}

func stringifyAddress(addr oid.Address) string {
	return addr.Object().EncodeToString() + "." + addr.Container().EncodeToString()
}

func addressFromString(s string) (*oid.Address, error) {
	objString, cnrString, found := strings.Cut(s, ".")
	if !found {
		return nil, errors.New("invalid address")
	}

	var obj oid.ID
	if err := obj.DecodeString(objString); err != nil {
		return nil, fmt.Errorf("decode object ID from string %q: %w", objString, err)
	}

	var cnr cid.ID
	if err := cnr.DecodeString(cnrString); err != nil {
		return nil, fmt.Errorf("decode container ID from string %q: %w", cnrString, err)
	}

	var addr oid.Address
	addr.SetObject(obj)
	addr.SetContainer(cnr)

	return &addr, nil
}

// Iterate iterates over all stored objects.
func (t *FSTree) Iterate(objHandler func(addr oid.Address, data []byte) error, errorHandler func(addr oid.Address, err error) error) error {
	return t.iterate(0, []string{t.RootPath}, objHandler, errorHandler, nil, nil)
}

// IterateAddresses iterates over all objects stored in the underlying storage
// and passes their addresses into f. If f returns an error, IterateAddresses
// returns it and breaks. ignoreErrors allows to continue if internal errors
// happen.
func (t *FSTree) IterateAddresses(f func(addr oid.Address) error, ignoreErrors bool) error {
	var errorHandler func(oid.Address, error) error
	if ignoreErrors {
		errorHandler = func(oid.Address, error) error { return nil }
	}

	return t.iterate(0, []string{t.RootPath}, nil, errorHandler, f, nil)
}

// IterateSizes iterates over all objects stored in the underlying storage
// and passes their addresses and sizes into f. If f returns an error, IterateSizes
// returns it and breaks. ignoreErrors allows to continue if internal errors
// happen.
func (t *FSTree) IterateSizes(f func(addr oid.Address, size uint64) error, ignoreErrors bool) error {
	var errorHandler func(oid.Address, error) error
	if ignoreErrors {
		errorHandler = func(oid.Address, error) error { return nil }
	}

	return t.iterate(0, []string{t.RootPath}, nil, errorHandler, nil, f)
}

func (t *FSTree) iterate(depth uint64, curPath []string,
	objHandler func(oid.Address, []byte) error,
	errorHandler func(oid.Address, error) error,
	addrHandler func(oid.Address) error,
	sizeHandler func(oid.Address, uint64) error) error {
	curName := strings.Join(curPath[1:], "")
	dir := filepath.Join(curPath...)
	des, err := os.ReadDir(dir)
	if err != nil {
		if errorHandler != nil {
			return errorHandler(oid.Address{}, err)
		}
		return fmt.Errorf("read dir %q: %w", dir, err)
	}

	isLast := depth >= t.Depth
	l := len(curPath)
	curPath = append(curPath, "")

	for i := range des {
		curPath[l] = des[i].Name()

		if !isLast && des[i].IsDir() {
			err := t.iterate(depth+1, curPath, objHandler, errorHandler, addrHandler, sizeHandler)
			if err != nil {
				// Must be error from handler in case errors are ignored.
				// Need to report.
				return err
			}
		}

		if depth != t.Depth {
			continue
		}

		addr, err := addressFromString(curName + des[i].Name())
		if err != nil {
			continue
		}

		if addrHandler != nil {
			err = addrHandler(*addr)
		} else {
			var data []byte
			p := filepath.Join(curPath...)
			if sizeHandler != nil {
				err = filepath.Walk(p, func(path string, info os.FileInfo, _ error) error {
					if !info.IsDir() {
						err = sizeHandler(*addr, uint64(info.Size()))
						if err != nil {
							return err
						}
					}
					return nil
				})
			} else {
				data, err = t.getObjectBytesByPath(addr.Object(), p)
				if err != nil {
					if errors.Is(err, apistatus.ErrObjectNotFound) {
						continue
					}
					if errorHandler != nil {
						err = errorHandler(*addr, err)
						if err == nil {
							continue
						}
					}
					return fmt.Errorf("read file %q: %w", p, err)
				}

				err = objHandler(*addr, data)
				if err != nil {
					err = fmt.Errorf("handling %s object: %w", addr, err)
				}
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *FSTree) treePath(addr oid.Address) string {
	sAddr := stringifyAddress(addr)

	dirs := make([]string, 0, t.Depth+1+1) // 1 for root, 1 for file
	dirs = append(dirs, t.RootPath)

	for i := 0; uint64(i) < t.Depth; i++ {
		dirs = append(dirs, sAddr[:DirNameLen])
		sAddr = sAddr[DirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return filepath.Join(dirs...)
}

// Delete removes the object with the specified address from the storage.
func (t *FSTree) Delete(addr oid.Address) error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	p, err := t.getPath(addr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return err
	}

	err = os.Remove(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return logicerr.Wrap(apistatus.ObjectNotFound{})
		}

		return fmt.Errorf("remove file %q: %w", p, err)
	}

	return nil
}

// Exists returns the path to the file with object contents if it exists in the storage
// and an error otherwise.
func (t *FSTree) Exists(addr oid.Address) (bool, error) {
	_, err := t.getPath(addr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// checks whether file for the given object address exists and returns path to
// the file if so. Returns [fs.ErrNotExist] if file is missing.
func (t *FSTree) getPath(addr oid.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	if err != nil {
		return "", fmt.Errorf("get filesystem path for object by address: get file stat %q: %w", p, err)
	}

	return p, nil
}

// Put puts an object in the storage.
func (t *FSTree) Put(addr oid.Address, data []byte) error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	p := t.treePath(addr)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return fmt.Errorf("mkdirall for %q: %w", p, err)
	}
	data = t.Compress(data)

	err := t.writer.writeData(addr.Object(), p, data)
	if err != nil {
		return fmt.Errorf("write object data into file %q: %w", p, err)
	}
	return nil
}

// PutBatch puts a batch of objects in the storage.
func (t *FSTree) PutBatch(objs map[oid.Address][]byte) error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	writeDataUnits := make([]writeDataUnit, 0, len(objs))
	for addr, data := range objs {
		p := t.treePath(addr)
		if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
			return fmt.Errorf("mkdirall for %q: %w", p, err)
		}
		writeDataUnits = append(writeDataUnits, writeDataUnit{
			id:   addr.Object(),
			path: p,
			data: t.Compress(data),
		})
	}

	err := t.writer.writeBatch(writeDataUnits)
	if err != nil {
		return fmt.Errorf("cannot write batch: %w", err)
	}

	return nil
}

// Get returns an object from the storage by address.
func (t *FSTree) Get(addr oid.Address) (*object.Object, error) {
	data, err := t.getObjBytes(addr)
	if err != nil {
		return nil, err
	}

	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("decode object: %w", err)
	}

	return obj, nil
}

// GetBytes reads object from the FSTree by address into memory buffer in a
// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (t *FSTree) GetBytes(addr oid.Address) ([]byte, error) {
	return t.getObjBytes(addr)
}

// getObjBytes extracts object bytes from the storage by address.
func (t *FSTree) getObjBytes(addr oid.Address) ([]byte, error) {
	p := t.treePath(addr)
	return t.getObjectBytesByPath(addr.Object(), p)
}

// getObjectBytesByPath extracts object bytes from the storage by path.
func (t *FSTree) getObjectBytesByPath(id oid.ID, p string) ([]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return nil, fmt.Errorf("read file %q: %w", p, err)
	}
	defer f.Close()
	data, err := t.extractCombinedObject(id, f)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return nil, fmt.Errorf("extract object from %q: %w", p, err)
	}
	return data, nil
}

// parseCombinedPrefix checks the given array for combined data prefix and
// returns a subslice with OID and object length if so (nil and 0 otherwise).
func parseCombinedPrefix(p []byte) ([]byte, uint32) {
	if len(p) < combinedDataOff || p[0] != combinedPrefix || p[1] != 0 { // Only version 0 is supported now.
		return nil, 0
	}
	return p[combinedIDOff:combinedLengthOff],
		binary.BigEndian.Uint32(p[combinedLengthOff:combinedDataOff])
}

func (t *FSTree) extractCombinedObject(id oid.ID, f *os.File) ([]byte, error) {
	var (
		comBuf     [combinedDataOff]byte
		isCombined bool
	)

	for {
		n, err := io.ReadFull(f, comBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if !isCombined {
					return t.Decompress(comBuf[:n])
				}
				return nil, fs.ErrNotExist
			}
			return nil, err
		}
		thisOID, l := parseCombinedPrefix(comBuf[:])
		if thisOID == nil {
			if isCombined {
				return nil, errors.New("malformed combined file")
			}
			st, err := f.Stat()
			if err != nil {
				return nil, err
			}
			sz := st.Size()
			if sz > math.MaxInt {
				return nil, errors.New("too large file")
			}
			return t.readFullObject(f, comBuf[:n], sz)
		}
		isCombined = true
		if bytes.Equal(thisOID, id[:]) {
			return t.readFullObject(f, nil, int64(l))
		}
		_, err = f.Seek(int64(l), 1)
		if err != nil {
			return nil, err
		}
	}
}

// readFullObject reads full data of object from the file and decompresses it if necessary.
func (t *FSTree) readFullObject(f io.Reader, initial []byte, size int64) ([]byte, error) {
	data := make([]byte, size)
	copy(data, initial)
	n, err := io.ReadFull(f, data[len(initial):])
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	data = data[:len(initial)+n]

	return t.Decompress(data)
}

// GetStream returns an object from the storage by address as a stream.
// It returns the object with header only, and a reader for the payload.
// On success, the reader is non-nil and must be closed;
// a nil reader is only returned with a non‑nil error.
func (t *FSTree) GetStream(addr oid.Address) (*object.Object, io.ReadCloser, error) {
	obj, reader, err := t.getObjectStream(addr)
	if err != nil {
		return nil, nil, err
	}

	return obj, reader, nil
}

// GetRange implements common.Storage.
func (t *FSTree) GetRange(addr oid.Address, from uint64, length uint64) ([]byte, error) {
	header, reader, err := t.getObjectStream(addr)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	pLen := header.PayloadSize()
	var to uint64
	if length != 0 {
		to = from + length
	} else {
		to = pLen
	}

	if to < from || pLen < from || pLen < to {
		return nil, logicerr.Wrap(apistatus.ErrObjectOutOfRange)
	}

	if from > 0 {
		_, err = reader.Seek(int64(from), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("seek to %d in stream: %w", from, err)
		}
	}

	payload := make([]byte, to-from)
	_, err = io.ReadFull(reader, payload)
	if err != nil {
		return nil, fmt.Errorf("read %d bytes from stream: %w", length, err)
	}

	return payload, nil
}

// Type is fstree storage type used in logs and configuration.
const Type = "fstree"

// Type implements common.Storage.
func (*FSTree) Type() string {
	return Type
}

// Path implements common.Storage.
func (t *FSTree) Path() string {
	return t.RootPath
}

// SetCompressor implements common.Storage.
func (t *FSTree) SetCompressor(cc *compression.Config) {
	t.Config = cc
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (t *FSTree) SetLogger(l *zap.Logger) {
	t.log = l.With(zap.String("substorage", Type))
}

// CleanUpTmp removes all temporary files garbage.
func (t *FSTree) CleanUpTmp() error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	err := filepath.WalkDir(t.RootPath,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.Contains(d.Name(), "#") {
				err = os.RemoveAll(path)
				if err != nil {
					return err
				}
			}

			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("could not walk through %q directory: %w", t.RootPath, err)
	}

	return nil
}
