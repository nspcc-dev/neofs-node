package fstree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"syscall"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	_ = iota
	fieldObjectID
	fieldObjectSignature
	fieldObjectHeader
	fieldObjectPayload
)

const smallObjectSizeLimit = 64 << 10 // 64 KB

// Head returns an object's header from the storage by address without reading the full payload.
func (t *FSTree) Head(addr oid.Address) (*objectSDK.Object, error) {
	p := t.treePath(addr)

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, fmt.Errorf("read file %q: %w", p, err)
	}
	defer f.Close()

	obj, err := t.extractHeaderOnly(addr.Object(), f, p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, fmt.Errorf("extract object header from %q: %w", p, err)
	}

	return obj, nil
}

// extractHeaderOnly reads the header of an object from a file.
func (t *FSTree) extractHeaderOnly(id oid.ID, f *os.File, path string) (*objectSDK.Object, error) {
	buf := make([]byte, objectSDK.MaxHeaderLen+combinedDataOff)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	buf = buf[:n]

	var comBuf [combinedDataOff]byte
	copy(comBuf[:], buf[:combinedDataOff])
	thisOID, l := parseCombinedPrefix(comBuf)
	if thisOID == nil {
		var stat syscall.Stat_t
		err = syscall.Stat(path, &stat)
		if err != nil {
			return nil, err
		}
		if stat.Size > math.MaxInt {
			return nil, errors.New("too large file")
		}
		return t.readObject(f, buf, stat.Size)
	}

	offset := combinedDataOff
	for {
		if bytes.Equal(thisOID, id[:]) {
			var initial []byte
			if offset < len(buf) {
				end := offset + int(l)
				if end > len(buf) {
					end = len(buf)
				}
				initial = buf[offset:end]
			}
			return t.readObject(f, initial, int64(l))
		}

		var skipBytes int64
		if offset >= len(buf) {
			skipBytes = int64(l)
		} else {
			if offset+int(l) > len(buf) {
				skipBytes = int64(l) - int64(len(buf)-offset)
			}
			offset += int(l)
		}
		if skipBytes > 0 {
			if _, err := f.Seek(skipBytes, io.SeekCurrent); err != nil {
				return nil, err
			}
		}

		if err := readCombinedPrefix(f, &comBuf, buf, offset); err != nil {
			return nil, err
		}

		thisOID, l = parseCombinedPrefix(comBuf)
		if thisOID == nil {
			return nil, errors.New("malformed combined file")
		}

		if offset < len(buf) {
			offset += combinedDataOff
		}
	}
}

// readCombinedPrefix reads the combined prefix from either buffer or file.
// It writes the result to the provided comBuf.
func readCombinedPrefix(f *os.File, comBuf *[combinedDataOff]byte, buf []byte, offset int) error {
	if offset+combinedDataOff < len(buf) {
		copy(comBuf[:], buf[offset:])
		return nil
	}

	var n int
	if offset < len(buf) {
		n = copy(comBuf[:], buf[offset:])
	}
	_, err := io.ReadFull(f, comBuf[n:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return err
	}
	return nil
}

// readObject reads an object from the file.
func (t *FSTree) readObject(f io.Reader, initial []byte, size int64) (*objectSDK.Object, error) {
	if size < smallObjectSizeLimit {
		data, err := t.readFullObject(f, initial, size)
		if err != nil {
			return nil, err
		}
		var obj objectSDK.Object
		err = obj.Unmarshal(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal object: %w", err)
		}
		return obj.CutPayload(), nil
	}
	return t.readUntilPayload(f, initial)
}

// readFullObject reads full data of object from the file and decompresses it if necessary.
func (t *FSTree) readFullObject(f io.Reader, initial []byte, size int64) ([]byte, error) {
	var data []byte
	if int64(len(initial)) >= size {
		data = initial
	} else {
		data = make([]byte, size)
		copy(data, initial)
		n, err := io.ReadFull(f, data[len(initial):])
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("read: %w", err)
		}
		data = data[:len(initial)+n]
	}

	return t.Decompress(data)
}

// readUntilPayload reads an object from the file until the payload field is reached.
func (t *FSTree) readUntilPayload(f io.Reader, initial []byte) (*objectSDK.Object, error) {
	var data []byte
	if int64(len(initial)) >= objectSDK.MaxHeaderLen {
		data = initial
	} else {
		data = make([]byte, objectSDK.MaxHeaderLen)
		copy(data, initial)

		n, err := io.ReadFull(f, data[len(initial):])
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("read: %w", err)
		}
		data = data[:len(initial)+n]
	}

	if t.IsCompressed(data) {
		decoder, err := zstd.NewReader(io.MultiReader(bytes.NewReader(data), f))
		if err != nil {
			return nil, fmt.Errorf("zstd decoder: %w", err)
		}
		defer decoder.Close()

		buf := make([]byte, objectSDK.MaxHeaderLen)
		n, err := decoder.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("zstd read: %w", err)
		}
		data = buf[:n]
	}

	return fastExtractHeader(data)
}

// fastExtractHeader extracts the header of an object from the given byte slice.
func fastExtractHeader(data []byte) (*objectSDK.Object, error) {
	var (
		offset int
		res    objectSDK.Object
		obj    object.Object
	)

	for offset < len(data) {
		num, typ, n := protowire.ConsumeTag(data[offset:])
		if err := protowire.ParseError(n); err != nil {
			return nil, fmt.Errorf("invalid tag at offset %d: %w", offset, err)
		}
		offset += n

		if typ != protowire.BytesType {
			return nil, fmt.Errorf("unexpected wire type: %v", typ)
		}

		if num == fieldObjectPayload {
			break
		}

		val, n := protowire.ConsumeBytes(data[offset:])
		if err := protowire.ParseError(n); err != nil {
			return nil, fmt.Errorf("invalid bytes field at offset %d: %w", offset, err)
		}
		offset += n

		switch num {
		case fieldObjectID:
			obj.ObjectId = new(refs.ObjectID)
			err := proto.Unmarshal(val, obj.ObjectId)
			if err != nil {
				return nil, fmt.Errorf("unmarshal object ID: %w", err)
			}
		case fieldObjectSignature:
			obj.Signature = new(refs.Signature)
			err := proto.Unmarshal(val, obj.Signature)
			if err != nil {
				return nil, fmt.Errorf("unmarshal object signature: %w", err)
			}
		case fieldObjectHeader:
			obj.Header = new(object.Header)
			err := proto.Unmarshal(val, obj.Header)
			if err != nil {
				return nil, fmt.Errorf("unmarshal object header: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown field number: %d", num)
		}
	}

	return &res, res.FromProtoMessage(&obj)
}
