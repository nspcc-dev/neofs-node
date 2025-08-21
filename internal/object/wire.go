package object

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/object"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Protobuf field numbers for object message.
const (
	_ = iota
	fieldObjectID
	fieldObjectSignature
	fieldObjectHeader
	fieldObjectPayload
)

// WriteWithoutPayload writes the object header to the given writer without the payload.
func WriteWithoutPayload(w io.Writer, obj object.Object) error {
	header := obj.CutPayload().Marshal()
	if obj.PayloadSize() != 0 {
		header = protowire.AppendTag(header, fieldObjectPayload, protowire.BytesType)
		header = protowire.AppendVarint(header, obj.PayloadSize())
	}
	_, err := w.Write(header)
	return err
}

// ExtractHeaderAndPayload extracts the header of an object from the given byte slice and also returns payload prefix.
func ExtractHeaderAndPayload(data []byte) (*object.Object, []byte, error) {
	var (
		offset int
		res    object.Object
		obj    protoobject.Object
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
			_, n = protowire.ConsumeVarint(data[offset:])
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
			if err := proto.Unmarshal(val, obj.ObjectId); err != nil {
				return nil, nil, fmt.Errorf("unmarshal object ID: %w", err)
			}
		case fieldObjectSignature:
			obj.Signature = new(refs.Signature)
			if err := proto.Unmarshal(val, obj.Signature); err != nil {
				return nil, nil, fmt.Errorf("unmarshal object signature: %w", err)
			}
		case fieldObjectHeader:
			obj.Header = new(protoobject.Header)
			if err := proto.Unmarshal(val, obj.Header); err != nil {
				return nil, nil, fmt.Errorf("unmarshal object header: %w", err)
			}
		default:
			return nil, nil, fmt.Errorf("unknown field number: %d", num)
		}
	}

	if err := res.FromProtoMessage(&obj); err != nil {
		return nil, nil, err
	}
	return &res, data[offset:], nil
}

// ReadHeaderPrefix reads up to [object.MaxHeaderLen] bytes and extracts header and the payload prefix.
func ReadHeaderPrefix(r io.Reader) (*object.Object, []byte, error) {
	buf := make([]byte, object.MaxHeaderLen)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		if errors.Is(err, io.EOF) {
			return nil, nil, io.ErrUnexpectedEOF
		}
		return nil, nil, err
	}
	return ExtractHeaderAndPayload(buf[:n])
}
