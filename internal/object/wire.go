package object

import (
	"errors"
	"fmt"
	"io"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// MaxHeaderVarintLen is varint len of [object.MaxHeaderLen].
// TODO: unit test.
const MaxHeaderVarintLen = 3 // object.MaxHeaderLen

// Protobuf field numbers for object message.
const (
	_ = iota
	fieldObjectID
	fieldObjectSignature
	fieldObjectHeader
	fieldObjectPayload
)

// Protobuf field numbers for header message.
const (
	_ = iota
	/* FieldHeaderVersion */ _
	/* FieldHeaderContainerID */ _
	/* FieldHeaderOwnerID */ _
	/* FieldHeaderCreationEpoch */ _
	/* FieldHeaderPayloadLength */ _
	/* FieldHeaderPayloadHash */ _
	/* FieldHeaderType */ _
	/* FieldHeaderHomoHash */ _
	/* FieldHeaderSessionToken */ _
	/* FieldHeaderAttributes */ _
	FieldHeaderSplit
	/* FieldHeaderSessionTokenV2 */ _
)

// Protobuf field numbers for split header message.
const (
	_ = iota
	FieldHeaderSplitParent
	FieldHeaderSplitPrevious
	FieldHeaderSplitParentSignature
	FieldHeaderSplitParentHeader
	/* FieldHeaderSplitChildren */ _
	/* FieldHeaderSplitSplitID */ _
	/* FieldHeaderSplitFirst */ _
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

// SeekParentHeaderFields seeks parent ID, signature and header in object
// message with direct field order.
func SeekParentHeaderFields(b []byte) (iprotobuf.FieldBounds, iprotobuf.FieldBounds, iprotobuf.FieldBounds, error) {
	var idf, sigf, hdrf iprotobuf.FieldBounds

	rootHdrf, err := iprotobuf.SeekBytesField(b, fieldObjectHeader)
	if err != nil {
		return idf, sigf, hdrf, err
	}

	if rootHdrf.IsMissing() {
		return idf, sigf, hdrf, nil
	}

	splitf, err := iprotobuf.SeekBytesField(b[rootHdrf.ValueFrom:rootHdrf.To], FieldHeaderSplit)
	if err != nil {
		return idf, sigf, hdrf, err
	}

	if splitf.IsMissing() {
		return idf, sigf, hdrf, nil
	}

	b = b[:rootHdrf.ValueFrom+splitf.To]
	off := rootHdrf.ValueFrom + splitf.ValueFrom
	var prevNum protowire.Number
loop:
	for {
		num, typ, n, err := iprotobuf.ParseTag(b, off)
		if err != nil {
			return idf, sigf, hdrf, err
		}

		if num < prevNum {
			return idf, sigf, hdrf, iprotobuf.NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum && num <= FieldHeaderSplitParentHeader {
			return idf, sigf, hdrf, iprotobuf.NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case FieldHeaderSplitParent:
			idf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			off = idf.To
		case FieldHeaderSplitPrevious:
			off += n
			ln, n, err := iprotobuf.ParseLenField(b, off, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			off += n + ln
		case FieldHeaderSplitParentSignature:
			sigf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			off = sigf.To
		case FieldHeaderSplitParentHeader:
			hdrf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			break loop
		default:
			break loop
		}

		if off == len(b) {
			break
		}
	}

	return idf, sigf, hdrf, nil
}

// SeekParentHeaderFields seeks ID, signature and header in object message with
// direct field order.
func SeekHeaderFields(b []byte) (iprotobuf.FieldBounds, iprotobuf.FieldBounds, iprotobuf.FieldBounds, error) {
	var idf, sigf, hdrf iprotobuf.FieldBounds
	var off int
	var prevNum protowire.Number
loop:
	for {
		num, typ, n, err := iprotobuf.ParseTag(b, off)
		if err != nil {
			return idf, sigf, hdrf, err
		}

		if num < prevNum {
			return idf, sigf, hdrf, iprotobuf.NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return idf, sigf, hdrf, iprotobuf.NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case fieldObjectID:
			idf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			off = idf.To
		case fieldObjectSignature:
			sigf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}
			off = sigf.To
		case fieldObjectHeader:
			hdrf, err = iprotobuf.ParseLenFieldBounds(b, off, n, num, typ)
			if err != nil {
				return idf, sigf, hdrf, err
			}

			break loop
		default:
			break loop
		}

		if off == len(b) {
			break
		}
	}

	return idf, sigf, hdrf, nil
}
