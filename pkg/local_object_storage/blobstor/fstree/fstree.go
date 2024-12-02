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
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// FSTree represents an object storage as a filesystem tree.
type FSTree struct {
	Info

	*compression.Config
	log        *zap.Logger
	Depth      uint64
	DirNameLen int
	writer     writer

	noSync   bool
	readOnly bool

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
		Config:     nil,
		Depth:      4,
		DirNameLen: DirNameLen,

		combinedCountLimit:    128,
		combinedSizeLimit:     8 * 1024 * 1024,
		combinedSizeThreshold: 128 * 1024,
		combinedWriteInterval: 10 * time.Millisecond,
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
	ss := strings.SplitN(s, ".", 2)
	if len(ss) != 2 {
		return nil, errors.New("invalid address")
	}

	var obj oid.ID
	if err := obj.DecodeString(ss[0]); err != nil {
		return nil, fmt.Errorf("decode object ID from string %q: %w", ss[0], err)
	}

	var cnr cid.ID
	if err := cnr.DecodeString(ss[1]); err != nil {
		return nil, fmt.Errorf("decode container ID from string %q: %w", ss[1], err)
	}

	var addr oid.Address
	addr.SetObject(obj)
	addr.SetContainer(cnr)

	return &addr, nil
}

// Iterate iterates over all stored objects.
func (t *FSTree) Iterate(objHandler func(addr oid.Address, data []byte, id []byte) error, errorHandler func(addr oid.Address, err error) error, ignoreErrors bool) error {
	return t.iterate(0, []string{t.RootPath}, objHandler, errorHandler, nil, ignoreErrors)
}

// IterateLazily is similar to Iterate, but allows to skip/defer object
// retrieval in the handler. Use getter function when needed.
func (t *FSTree) IterateLazily(lazyHandler func(addr oid.Address, getter func() ([]byte, error)) error, ignoreErrors bool) error {
	return t.iterate(0, []string{t.RootPath}, nil, nil, lazyHandler, ignoreErrors)
}

func (t *FSTree) iterate(depth uint64, curPath []string,
	objHandler func(oid.Address, []byte, []byte) error,
	errorHandler func(oid.Address, error) error,
	lazyHandler func(oid.Address, func() ([]byte, error)) error,
	ignoreErrors bool) error {
	curName := strings.Join(curPath[1:], "")
	dir := filepath.Join(curPath...)
	des, err := os.ReadDir(dir)
	if err != nil {
		if ignoreErrors {
			return nil
		}
		return fmt.Errorf("read dir %q: %w", dir, err)
	}

	isLast := depth >= t.Depth
	l := len(curPath)
	curPath = append(curPath, "")

	for i := range des {
		curPath[l] = des[i].Name()

		if !isLast && des[i].IsDir() {
			err := t.iterate(depth+1, curPath, objHandler, errorHandler, lazyHandler, ignoreErrors)
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

		if lazyHandler != nil {
			err = lazyHandler(*addr, func() ([]byte, error) {
				return getRawObjectBytes(addr.Object(), filepath.Join(curPath...))
			})
		} else {
			var data []byte
			p := filepath.Join(curPath...)
			data, err = getRawObjectBytes(addr.Object(), p)
			if err != nil && errors.Is(err, apistatus.ObjectNotFound{}) {
				continue
			}
			if err == nil {
				data, err = t.Decompress(data)
			}
			if err != nil {
				if ignoreErrors {
					if errorHandler != nil {
						return errorHandler(*addr, err)
					}
					continue
				}
				return fmt.Errorf("read file %q: %w", p, err)
			}

			err = objHandler(*addr, data, []byte{})
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
		dirs = append(dirs, sAddr[:t.DirNameLen])
		sAddr = sAddr[t.DirNameLen:]
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
func (t *FSTree) Put(prm common.PutPrm) (common.PutRes, error) {
	if t.readOnly {
		return common.PutRes{}, common.ErrReadOnly
	}

	p := t.treePath(prm.Address)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return common.PutRes{}, fmt.Errorf("mkdirall for %q: %w", p, err)
	}
	prm.RawData = t.Compress(prm.RawData)

	err := t.writer.writeData(prm.Address.Object(), p, prm.RawData)
	if err != nil {
		return common.PutRes{}, fmt.Errorf("write object data into file %q: %w", p, err)
	}
	return common.PutRes{StorageID: []byte{}}, nil
}

// Get returns an object from the storage by address.
func (t *FSTree) Get(addr oid.Address) (*objectSDK.Object, error) {
	data, err := t.getObjBytes(addr)
	if err != nil {
		return nil, err
	}

	obj := objectSDK.New()
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
	data, err := getRawObjectBytes(addr.Object(), p)
	if err != nil {
		return nil, err
	}
	data, err = t.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompress file data %q: %w", p, err)
	}
	return data, nil
}

// getRawObjectBytes extracts raw object bytes from the storage by path. No
// decompression is performed.
func getRawObjectBytes(id oid.ID, p string) ([]byte, error) {
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return nil, fmt.Errorf("read file %q: %w", p, err)
	}
	defer f.Close()
	data, err := extractCombinedObject(id, f)
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
func parseCombinedPrefix(p [combinedDataOff]byte) ([]byte, uint32) {
	if p[0] != combinedPrefix || p[1] != 0 { // Only version 0 is supported now.
		return nil, 0
	}
	return p[combinedIDOff:combinedLengthOff],
		binary.BigEndian.Uint32(p[combinedLengthOff:combinedDataOff])
}

func extractCombinedObject(id oid.ID, f *os.File) ([]byte, error) {
	var (
		comBuf     [combinedDataOff]byte
		data       []byte
		isCombined bool
	)

	for {
		n, err := io.ReadFull(f, comBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if !isCombined {
					return comBuf[:n], nil
				}
				return nil, fs.ErrNotExist
			}
			return nil, err
		}
		thisOID, l := parseCombinedPrefix(comBuf)
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
			data = make([]byte, int(sz))
			copy(data, comBuf[:])
			_, err = io.ReadFull(f, data[len(comBuf):])
			if err != nil {
				return nil, err
			}
			return data, nil
		}
		isCombined = true
		if bytes.Equal(thisOID, id[:]) {
			data = make([]byte, l)
			_, err = io.ReadFull(f, data)
			if err != nil {
				return nil, err
			}
			return data, nil
		}
		_, err = f.Seek(int64(l), 1)
		if err != nil {
			return nil, err
		}
	}
}

// GetRange implements common.Storage.
func (t *FSTree) GetRange(addr oid.Address, from uint64, length uint64) ([]byte, error) {
	obj, err := t.Get(addr)
	if err != nil {
		return nil, err
	}

	payload := obj.Payload()
	to := from + length

	if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
		return nil, logicerr.Wrap(apistatus.ObjectOutOfRange{})
	}

	return payload[from:to], nil
}

// NumberOfObjects walks the file tree rooted at FSTree's root
// and returns number of stored objects.
func (t *FSTree) NumberOfObjects() (uint64, error) {
	var counter uint64

	// it is simpler to just consider every file
	// that is not directory as an object
	err := filepath.WalkDir(t.RootPath,
		func(_ string, d fs.DirEntry, _ error) error {
			if !d.IsDir() {
				counter++
			}

			return nil
		},
	)
	if err != nil {
		return 0, fmt.Errorf("could not walk through %q directory: %w", t.RootPath, err)
	}

	return counter, nil
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
