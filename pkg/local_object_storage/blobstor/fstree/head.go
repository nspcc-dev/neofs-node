package fstree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

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

	obj, err := t.extractHeaderOnly(addr.Object(), f)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, fmt.Errorf("extract object header from %q: %w", p, err)
	}

	return obj, nil
}

// extractHeaderOnly reads the header of an object from a file.
func (t *FSTree) extractHeaderOnly(id oid.ID, f *os.File) (*objectSDK.Object, error) {
	buf := make([]byte, objectSDK.MaxHeaderLen, 2*objectSDK.MaxHeaderLen)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	thisOID, l := parseCombinedPrefix(buf)
	if thisOID == nil {
		return t.readHeader(f, buf[:n])
	}

	offset := combinedDataOff
	for {
		if bytes.Equal(thisOID, id[:]) {
			size := min(offset+int(l), offset+objectSDK.MaxHeaderLen)
			if n < size {
				_, err = io.ReadFull(f, buf[n:size])
				if err != nil {
					return nil, fmt.Errorf("read up to size: %w", err)
				}
			}
			return t.readHeader(f, buf[offset:size])
		}

		offset += int(l)
		if n-offset < combinedDataOff {
			if offset > n {
				_, err = f.Seek(int64(offset-n), io.SeekCurrent)
				if err != nil {
					return nil, err
				}
			}
			n = copy(buf, buf[min(offset, n):n])
			offset = 0
			k, err := io.ReadFull(f, buf[n:n+objectSDK.MaxHeaderLen])
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("read full: %w", err)
			}
			n += k
		}

		thisOID, l = parseCombinedPrefix(buf[offset:])
		if thisOID == nil {
			return nil, errors.New("malformed combined file")
		}

		offset += combinedDataOff
	}
}

// readHeader reads an object from the file.
func (t *FSTree) readHeader(f io.Reader, initial []byte) (*objectSDK.Object, error) {
	var err error
	if len(initial) < objectSDK.MaxHeaderLen {
		initial, err = t.Decompress(initial)
		if err != nil {
			return nil, fmt.Errorf("decompress initial data: %w", err)
		}
		var obj objectSDK.Object
		err = obj.Unmarshal(initial)
		if err != nil {
			return nil, fmt.Errorf("unmarshal object: %w", err)
		}
		return obj.CutPayload(), nil
	}
	return t.readUntilPayload(f, initial)
}

// readUntilPayload reads an object from the file until the payload field is reached.
func (t *FSTree) readUntilPayload(f io.Reader, initial []byte) (*objectSDK.Object, error) {
	if t.IsCompressed(initial) {
		decoder, err := zstd.NewReader(io.MultiReader(bytes.NewReader(initial), f))
		if err != nil {
			return nil, fmt.Errorf("zstd decoder: %w", err)
		}
		defer decoder.Close()

		buf := make([]byte, objectSDK.MaxHeaderLen)
		n, err := decoder.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("zstd read: %w", err)
		}
		initial = buf[:n]
	}

	return fastExtractHeader(initial)
}

// fastExtractHeader extracts the header of an object from the given byte slice.
func fastExtractHeader(data []byte) (*objectSDK.Object, error) {
	var (
		offset int
		res    objectSDK.Object
		obj    object.Object
	)

	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

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
			return &res, res.FromProtoMessage(&obj)
		default:
			return nil, fmt.Errorf("unknown field number: %d", num)
		}
	}

	return &res, res.FromProtoMessage(&obj)
}
