package fstree

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	objectwire "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
)

// FSTree represents an object storage as a filesystem tree.
type FSTree struct {
	Info

	log              *zap.Logger
	Depth            uint64
	secondaryDepth   uint64
	AllowDepthChange bool
	writer           writer

	depthSet   bool
	shardIDSet bool
	subtypeSet bool

	noSync     bool
	readOnly   bool
	shardID    common.ID
	subtype    string
	descriptor fsDescriptor

	combinedCountLimit    int
	combinedSizeLimit     int
	combinedSizeThreshold int
	combinedWriteInterval time.Duration

	reshapeStateMtx sync.Mutex
	reshapeCancel   func()
	reshapeDone     chan struct{}
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

const SubtypeBlobstor = "blobstor"

func New(opts ...Option) *FSTree {
	f := &FSTree{
		Info: Info{
			Permissions: 0700,
			RootPath:    "./",
		},
		Depth: 4,

		combinedCountLimit:    128,
		combinedSizeLimit:     8 * 1024 * 1024,
		combinedSizeThreshold: 128 * 1024,
		combinedWriteInterval: 10 * time.Millisecond,
		log:                   zap.NewNop(),
		subtype:               SubtypeBlobstor,
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
	return t.iterateMerged(objHandler, errorHandler, nil, nil)
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
	return t.iterateMerged(nil, errorHandler, f, nil)
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
	return t.iterateMerged(nil, errorHandler, nil, f)
}

type layoutEntry struct {
	addr oid.Address
	path string
}

type layoutIterator struct {
	treeDepth uint64
	frames    []layoutIteratorFrame
}

type layoutIteratorFrame struct {
	depth   uint64
	dir     string
	prefix  string
	entries []os.DirEntry
	next    int
}

func newLayoutIterator(root string, treeDepth uint64) (*layoutIterator, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", root, err)
	}
	return &layoutIterator{treeDepth: treeDepth, frames: []layoutIteratorFrame{{dir: root, entries: entries}}}, nil
}

func (i *layoutIterator) next() (layoutEntry, bool, error) {
	for len(i.frames) > 0 {
		frame := &i.frames[len(i.frames)-1]
		if frame.next == len(frame.entries) {
			i.frames = i.frames[:len(i.frames)-1]
			continue
		}

		entry := frame.entries[frame.next]
		frame.next++
		path := filepath.Join(frame.dir, entry.Name())
		if frame.depth < i.treeDepth {
			if !entry.IsDir() {
				continue
			}
			entries, err := os.ReadDir(path)
			if err != nil {
				return layoutEntry{}, false, fmt.Errorf("read dir %q: %w", path, err)
			}
			i.frames = append(i.frames, layoutIteratorFrame{depth: frame.depth + 1, dir: path, prefix: frame.prefix + entry.Name(), entries: entries})
			continue
		}
		if entry.IsDir() {
			continue
		}
		addr, err := addressFromString(frame.prefix + entry.Name())
		if err != nil {
			continue
		}
		return layoutEntry{addr: *addr, path: path}, true, nil
	}
	return layoutEntry{}, false, nil
}

func nextLayoutEntry(i *layoutIterator, errorHandler func(oid.Address, error) error) (layoutEntry, bool, error) {
	for {
		entry, ok, err := i.next()
		if err == nil {
			return entry, ok, nil
		}
		if errorHandler == nil {
			return layoutEntry{}, false, err
		}
		if err = errorHandler(oid.Address{}, err); err != nil {
			return layoutEntry{}, false, err
		}
	}
}

func (t *FSTree) iterateMerged(objHandler func(oid.Address, []byte) error, errorHandler func(oid.Address, error) error, addrHandler func(oid.Address) error, sizeHandler func(oid.Address, uint64) error) error {
	primary, err := newLayoutIterator(t.RootPath, t.Depth)
	if err != nil {
		if errorHandler != nil {
			return errorHandler(oid.Address{}, err)
		}
		return err
	}
	var secondary *layoutIterator
	if t.secondaryDepth != 0 && t.secondaryDepth != t.Depth {
		secondary, err = newLayoutIterator(t.RootPath, t.secondaryDepth)
		if err != nil {
			if errorHandler != nil {
				return errorHandler(oid.Address{}, err)
			}
			return err
		}
	}

	primaryEntry, primaryOK, err := nextLayoutEntry(primary, errorHandler)
	if err != nil {
		return err
	}
	var secondaryEntry layoutEntry
	var secondaryOK bool
	if secondary != nil {
		secondaryEntry, secondaryOK, err = nextLayoutEntry(secondary, errorHandler)
		if err != nil {
			return err
		}
	}

	for primaryOK || secondaryOK {
		entry := primaryEntry
		advancePrimary, advanceSecondary := false, false
		if !primaryOK {
			entry = secondaryEntry
			advanceSecondary = true
		} else if !secondaryOK {
			advancePrimary = true
		} else {
			switch strings.Compare(stringifyAddress(primaryEntry.addr), stringifyAddress(secondaryEntry.addr)) {
			case -1:
				advancePrimary = true
			case 1:
				entry = secondaryEntry
				advanceSecondary = true
			default:
				advancePrimary, advanceSecondary = true, true
			}
		}

		if addrHandler != nil {
			if err = addrHandler(entry.addr); err != nil {
				return err
			}
		} else if sizeHandler != nil {
			info, statErr := os.Stat(entry.path)
			if statErr != nil {
				if errorHandler == nil {
					return fmt.Errorf("stat file %q: %w", entry.path, statErr)
				}
				if err = errorHandler(entry.addr, statErr); err != nil {
					return fmt.Errorf("stat file %q: %w", entry.path, err)
				}
			} else if err = sizeHandler(entry.addr, uint64(info.Size())); err != nil {
				return err
			}
		} else {
			data, readErr := t.getObjBytes(entry.addr)
			if readErr != nil {
				if !errors.Is(readErr, apistatus.ErrObjectNotFound) {
					if errorHandler == nil {
						return fmt.Errorf("read file %q: %w", entry.path, readErr)
					}
					if err = errorHandler(entry.addr, readErr); err != nil {
						return fmt.Errorf("read file %q: %w", entry.path, err)
					}
				}
			} else if err = objHandler(entry.addr, data); err != nil {
				return fmt.Errorf("handling %s object: %w", entry.addr, err)
			}
		}

		if advancePrimary {
			primaryEntry, primaryOK, err = nextLayoutEntry(primary, errorHandler)
			if err != nil {
				return err
			}
		}
		if advanceSecondary {
			secondaryEntry, secondaryOK, err = nextLayoutEntry(secondary, errorHandler)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *FSTree) treePath(addr oid.Address) string {
	return t.treePathAtDepth(addr, t.Depth)
}

func (t *FSTree) treePathAtDepth(addr oid.Address, depth uint64) string {
	sAddr := stringifyAddress(addr)

	dirs := make([]string, 0, depth+1+1) // 1 for root, 1 for file
	dirs = append(dirs, t.RootPath)

	for range depth {
		dirs = append(dirs, sAddr[:DirNameLen])
		sAddr = sAddr[DirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return filepath.Join(dirs...)
}

func (t *FSTree) treePaths(addr oid.Address) (string, string) {
	primary := t.treePath(addr)
	if t.secondaryDepth == 0 || t.secondaryDepth == t.Depth {
		return primary, ""
	}
	return primary, t.treePathAtDepth(addr, t.secondaryDepth)
}

// Delete removes the object with the specified address from the storage.
func (t *FSTree) Delete(addr oid.Address) error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	var removed bool
	primary, secondary := t.treePaths(addr)
	for _, p := range [...]string{secondary, primary} {
		if p == "" {
			continue
		}
		err := os.Remove(p)
		if err == nil {
			removed = true
			continue
		}
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		return fmt.Errorf("remove file %q: %w", p, err)
	}
	if !removed {
		return logicerr.Wrap(apistatus.ObjectNotFound{})
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
	primary, secondary := t.treePaths(addr)
	for _, p := range [...]string{secondary, primary} {
		if p == "" {
			continue
		}
		_, err := os.Stat(p)
		if err == nil {
			return p, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("get filesystem path for object by address: get file stat %q: %w", p, err)
		}
	}

	return "", fmt.Errorf("get filesystem path for object by address: get file stat %q: %w", t.treePath(addr), fs.ErrNotExist)
}

// Put puts an object in the storage.
func (t *FSTree) Put(addr oid.Address, data []byte) error {
	if t.readOnly {
		return common.ErrReadOnly
	}
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}

	p := t.treePath(addr)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return fmt.Errorf("mkdirall for %q: %w", p, err)
	}

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
		if len(data) == 0 {
			continue
		}
		p := t.treePath(addr)
		if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
			return fmt.Errorf("mkdirall for %q: %w", p, err)
		}
		writeDataUnits = append(writeDataUnits, writeDataUnit{
			id:   addr.Object(),
			path: p,
			data: data,
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

	obj := new(object.Object)
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
	primary, secondary := t.treePaths(addr)
	for _, p := range [...]string{secondary, primary} {
		if p == "" {
			continue
		}
		data, err := t.getObjectBytesByPath(addr.Object(), p)
		if err == nil || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return data, err
		}
	}
	return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
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
					return decompress(comBuf[:n])
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
			if l == 0 {
				return nil, io.ErrUnexpectedEOF
			}
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

	return decompress(data)
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

// GetRangeStream reads the requested payload range of the referenced object
// from t. It optionally returns the object header parsed from the same read.
// The stream must be finally closed by the caller.
//
// If object is missing, GetRangeStream returns [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStream returns
// [apistatus.ErrObjectOutOfRange].
func (t *FSTree) GetRangeStream(addr oid.Address, rng common.PayloadRange, readHeader bool) (*object.Object, uint64, io.ReadCloser, error) {
	return t.readPayloadRange(addr, rng, readHeader, nil, func() []byte {
		return make([]byte, 2*objectwire.NonPayloadFieldsBufferLength)
	})
}

// ReadPayloadRange is [FSTree.ReadObject] analogue for payload range reading.
// Zero range means full payload.
//
// If given range is out of payload bounds, ReadPayloadRange returns
// [apistatus.ErrObjectOutOfRange].
//
// If interceptHeaderBinaryFn is specified, it's called instantly once header is
// read (never concurrently). If it returns an error, whole operation is aborted
// with this error.
func (t *FSTree) ReadPayloadRange(addr oid.Address, off, ln uint64, hdrBuf []byte, interceptHeaderBinaryFn func([]byte) error) (io.ReadCloser, error) {
	_, _, stream, err := t.readPayloadRange(addr, common.NewPayloadRange(off, ln), false, interceptHeaderBinaryFn, func() []byte {
		return hdrBuf
	})
	return stream, err
}

func (t *FSTree) readPayloadRange(addr oid.Address, rng common.PayloadRange, readHeader bool, interceptHeaderBinaryFn func([]byte) error, getHdrBuf func() []byte) (*object.Object, uint64, io.ReadCloser, error) {
	prefix, stream, err := t._readObject(addr, getHdrBuf())
	if err != nil {
		return nil, 0, nil, err
	}

	if stream != nil {
		defer func() {
			if err != nil {
				stream.Close()
			}
		}()
	}

	// TODO: traverse buffer at once
	hf, err := iprotobuf.GetLENFieldBounds(prefix, protoobject.FieldObjectHeader)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("seek header field: %w", err)
	}

	var pldLen uint64
	if !hf.IsMissing() {
		hdrBin := prefix[hf.ValueFrom:hf.To]
		if interceptHeaderBinaryFn != nil {
			if err = interceptHeaderBinaryFn(hdrBin); err != nil {
				return nil, 0, nil, err
			}
		}
		pldLen, err = objectwire.GetPayloadLengthHeader(hdrBin)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("seek payload length field in header: %w", err)
		}
	}

	resStream, err := shiftStreamToRange(prefix, pldLen, rng, stream)
	if err != nil {
		return nil, 0, nil, err
	}

	var hdr *object.Object
	if readHeader {
		hdr, _, err = objectwire.ExtractHeaderAndPayload(prefix)
		if err != nil {
			return nil, pldLen, nil, fmt.Errorf("extract header in read payload range: %w", err)
		}
	}

	return hdr, pldLen, resStream, nil
}

func shiftStreamToRange(prefix []byte, pldLen uint64, rng common.PayloadRange, stream io.ReadSeekCloser) (io.ReadSeekCloser, error) {
	pldFldOff, pldFldTagLn, typ, err := iprotobuf.SeekFieldByNumber(prefix, protoobject.FieldObjectPayload)
	if err != nil {
		return nil, fmt.Errorf("seek payload field: %w", err)
	}
	if pldFldOff >= 0 && typ != protowire.BytesType {
		return nil, fmt.Errorf("wrong payload field type: expected %d, got %d", protowire.BytesType, typ)
	}

	off, ln, err := rng.Resolve(pldLen)
	if err != nil {
		return nil, err
	}

	if pldFldOff >= 0 {
		pldFldOff += pldFldTagLn
	}

	return shiftPayloadRangeStream(prefix, pldLen, pldFldOff, stream, off, ln)
}

func shiftPayloadRangeStream(prefix []byte, pldLen uint64, pldFldOff int, stream io.ReadSeekCloser, off, ln uint64) (io.ReadSeekCloser, error) {
	if pldFldOff < 0 {
		if pldLen != 0 {
			return nil, fmt.Errorf("missing payload field tag in %d bytes header, payload len in header = %d", len(prefix), pldLen)
		}
		if stream != nil {
			stream.Close()
		}
		return nopReadCloser{}, nil
	}

	if _, n, err := iprotobuf.ParseVarint(prefix[pldFldOff:]); err == nil {
		prefix = prefix[pldFldOff+n:]
	} else {
		if stream == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("parse payload field len: %w", err)
		}

		if len(prefix) >= binary.MaxVarintLen64 {
			n = copy(prefix, prefix[pldFldOff:])
		} else { // unlikely to happen
			tmp := prefix
			prefix = make([]byte, binary.MaxVarintLen64)
			n = copy(prefix, tmp[pldFldOff:])
		}

		extra, err := io.ReadFull(stream, prefix[n:])
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read stream: %w", err)
		}

		_, n, err = iprotobuf.ParseVarint(prefix[:n+extra])
		if err != nil {
			return nil, fmt.Errorf("parse payload field len: %w", err)
		}

		prefix = prefix[:n]
	}

	if stream == nil && uint64(len(prefix)) != pldLen {
		return nil, fmt.Errorf("diff len of object payload: in header %d, in field tag %d", pldLen, len(prefix))
	}

	// check range is already buffered

	if off == 0 {
		if ln == 0 { // full
			if stream == nil {
				return nopCloser(bytes.NewReader(prefix)), nil
			}
			if len(prefix) == 0 {
				return stream, nil
			}
			return newPrefixedReadSeekCloser(prefix, stream), nil
		}

		if ln <= uint64(len(prefix)) {
			if stream != nil {
				stream.Close()
			}
			return nopCloser(bytes.NewReader(prefix[:ln])), nil
		}

		// stream is non-nil here according to conditions above

		if err := checkTooBigRange(off, ln); err != nil {
			return nil, err
		}

		if len(prefix) == 0 {
			return &limitedFileReader{ReadSeekCloser: stream, limit: int64(ln)}, nil
		}

		return newPrefixedReadSeekCloser(prefix, &limitedFileReader{ReadSeekCloser: stream, limit: int64(ln) - int64(len(prefix))}), nil
	}

	if stream == nil {
		// range is within slice according to conditions above
		return nopCloser(bytes.NewReader(prefix[off:][:ln])), nil
	}

	if err := checkTooBigRange(off, ln); err != nil {
		return nil, err
	}

	if off >= uint64(len(prefix)) {
		if off > uint64(len(prefix)) {
			_, err := stream.Seek(int64(off)-int64(len(prefix)), io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("seek payload stream: %w", err)
			}
		}
		return &limitedFileReader{ReadSeekCloser: stream, limit: int64(ln)}, nil
	}

	prefix = prefix[off:]
	if ln <= uint64(len(prefix)) {
		stream.Close()
		return nopCloser(bytes.NewReader(prefix[:ln])), nil
	}

	return newPrefixedReadSeekCloser(prefix, &limitedFileReader{ReadSeekCloser: stream, limit: int64(ln) - int64(len(prefix))}), nil
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

// ShardID returns the shard ID associated with this FSTree.
func (t *FSTree) ShardID() common.ID {
	if !t.shardIDSet {
		descPath := t.descriptorPath()
		f, err := os.Open(descPath)
		if err != nil {
			return common.ID{}
		}
		defer f.Close()

		var d fsDescriptor
		dec := json.NewDecoder(f)
		dec.DisallowUnknownFields()
		if err = dec.Decode(&d); err != nil {
			return common.ID{}
		}
		id, err := common.DecodeIDString(d.ShardID)
		if err != nil {
			return common.ID{}
		}
		return id
	}
	return t.shardID
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
