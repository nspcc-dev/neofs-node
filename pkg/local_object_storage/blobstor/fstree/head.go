package fstree

import (
	"bytes"
	"encoding/binary"
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
	obj, reader, err := t.getObjectStream(addr)
	if err != nil {
		return nil, err
	}
	_ = reader.Close()

	return obj, nil
}

// getObjectStream reads an object from the storage by address as a stream.
// It returns the object with header only, and a reader for the payload.
func (t *FSTree) getObjectStream(addr oid.Address) (*objectSDK.Object, io.ReadSeekCloser, error) {
	p := t.treePath(addr)

	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, nil, fmt.Errorf("read file %q: %w", p, err)
	}

	obj, reader, err := t.extractHeaderAndStream(addr.Object(), f)
	if err != nil {
		if reader != nil {
			_ = reader.Close()
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, logicerr.Wrap(apistatus.ErrObjectNotFound)
		}
		return nil, nil, fmt.Errorf("extract object stream from %q: %w", p, err)
	}

	return obj, reader, nil
}

// extractHeaderAndStream reads the header of an object from a file.
// The caller is responsible for closing the returned io.ReadCloser if it is not nil.
func (t *FSTree) extractHeaderAndStream(id oid.ID, f *os.File) (*objectSDK.Object, io.ReadSeekCloser, error) {
	buf := make([]byte, 2*objectSDK.MaxHeaderLen)
	n, err := io.ReadFull(f, buf[:objectSDK.MaxHeaderLen])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, f, err
	}
	if n < combinedDataOff {
		return t.readHeaderAndPayload(f, buf[:n])
	}

	thisOID, l := parseCombinedPrefix(buf)
	if thisOID == nil {
		return t.readHeaderAndPayload(f, buf[:n])
	}

	offset := combinedDataOff
	for {
		if bytes.Equal(thisOID, id[:]) {
			size := min(offset+int(l), offset+objectSDK.MaxHeaderLen)
			if n < size {
				_, err = io.ReadFull(f, buf[n:size])
				if err != nil {
					return nil, f, fmt.Errorf("read up to size: %w", err)
				}
			}
			return t.readHeaderAndPayload(f, buf[offset:size])
		}

		offset += int(l)
		if n-offset < combinedDataOff {
			if offset > n {
				_, err = f.Seek(int64(offset-n), io.SeekCurrent)
				if err != nil {
					return nil, f, err
				}
			}
			n = copy(buf, buf[min(offset, n):n])
			offset = 0
			k, err := io.ReadFull(f, buf[n:n+objectSDK.MaxHeaderLen])
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, f, fmt.Errorf("read full: %w", err)
			}
			n += k
		}

		thisOID, l = parseCombinedPrefix(buf[offset:])
		if thisOID == nil {
			return nil, f, errors.New("malformed combined file")
		}

		offset += combinedDataOff
	}
}

// readHeaderAndPayload reads an object header from the file and returns reader for payload.
// This function takes ownership of the io.ReadCloser and will close it if it does not return it.
func (t *FSTree) readHeaderAndPayload(f io.ReadCloser, initial []byte) (*objectSDK.Object, io.ReadSeekCloser, error) {
	var err error
	if len(initial) < objectSDK.MaxHeaderLen {
		_ = f.Close()
		initial, err = t.Decompress(initial)
		if err != nil {
			return nil, nil, fmt.Errorf("decompress initial data: %w", err)
		}
		var obj objectSDK.Object
		err = obj.Unmarshal(initial)
		if err != nil {
			return nil, nil, fmt.Errorf("unmarshal object: %w", err)
		}
		return obj.CutPayload(), &payloadReader{
			Reader: bytes.NewReader(obj.Payload()),
			close:  func() error { return nil },
		}, nil
	}

	return t.readUntilPayload(f, initial)
}

// readUntilPayload reads an object from the file until the payload field is reached
// and returns the object along with a reader for the remaining data.
// This function takes ownership of the io.ReadCloser and will close it if it does not return it.
func (t *FSTree) readUntilPayload(f io.ReadCloser, initial []byte) (*objectSDK.Object, io.ReadSeekCloser, error) {
	reader := f

	if t.IsCompressed(initial) {
		decoder, err := zstd.NewReader(io.MultiReader(bytes.NewReader(initial), f))
		if err != nil {
			return nil, nil, fmt.Errorf("zstd decoder: %w", err)
		}
		reader = decoder.IOReadCloser()

		buf := make([]byte, objectSDK.MaxHeaderLen)
		n, err := decoder.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			decoder.Close()
			return nil, nil, fmt.Errorf("zstd read: %w", err)
		}
		initial = buf[:n]
	}

	obj, rest, err := extractHeaderAndPayload(initial)
	if err != nil {
		_ = reader.Close()
		return nil, nil, fmt.Errorf("extract header and payload: %w", err)
	}

	return obj, &payloadReader{
		Reader: io.MultiReader(bytes.NewReader(rest), reader),
		close:  reader.Close,
	}, nil
}

// extractHeaderAndPayload extracts the header of an object from the given byte slice and returns rest of the data.
func extractHeaderAndPayload(data []byte) (*objectSDK.Object, []byte, error) {
	var (
		offset int
		res    objectSDK.Object
		obj    object.Object
	)

	if len(data) == 0 {
		return nil, nil, fmt.Errorf("empty data")
	}

	for offset < len(data) {
		num, typ, n := protowire.ConsumeTag(data[offset:])
		if err := protowire.ParseError(n); err != nil {
			return nil, nil, fmt.Errorf("invalid tag at offset %d: %w", offset, err)
		}
		offset += n

		if typ != protowire.BytesType {
			return nil, nil, fmt.Errorf("unexpected wire type: %v", typ)
		}

		if num == fieldObjectPayload {
			_, n = binary.Varint(data[offset:])
			if err := protowire.ParseError(n); err != nil {
				return nil, nil, fmt.Errorf("invalid varint at offset %d: %w", offset, err)
			}
			offset += n
			break
		}
		val, n := protowire.ConsumeBytes(data[offset:])
		if err := protowire.ParseError(n); err != nil {
			return nil, nil, fmt.Errorf("invalid bytes field at offset %d: %w", offset, err)
		}
		offset += n

		switch num {
		case fieldObjectID:
			obj.ObjectId = new(refs.ObjectID)
			err := proto.Unmarshal(val, obj.ObjectId)
			if err != nil {
				return nil, nil, fmt.Errorf("unmarshal object ID: %w", err)
			}
		case fieldObjectSignature:
			obj.Signature = new(refs.Signature)
			err := proto.Unmarshal(val, obj.Signature)
			if err != nil {
				return nil, nil, fmt.Errorf("unmarshal object signature: %w", err)
			}
		case fieldObjectHeader:
			obj.Header = new(object.Header)
			err := proto.Unmarshal(val, obj.Header)
			if err != nil {
				return nil, nil, fmt.Errorf("unmarshal object header: %w", err)
			}
		default:
			return nil, nil, fmt.Errorf("unknown field number: %d", num)
		}
	}

	return &res, data[offset:], res.FromProtoMessage(&obj)
}

type payloadReader struct {
	io.Reader
	close func() error
}

func (p *payloadReader) Close() error {
	return p.close()
}

// Seek implements io.Seeker interface for payloadReader.
// If the Reader does not support seeking, it will discard the data until the offset
// is reached, but only if whence is io.SeekStart. If whence is not io.SeekStart,
// it returns an error indicating that seeking is not supported.
func (p *payloadReader) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := p.Reader.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	if whence == io.SeekStart {
		return io.CopyN(io.Discard, p.Reader, offset)
	}
	return 0, errors.New("payload reader does not support seeking")
}
