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
	/* fieldHeaderVersion */ _
	/* fieldHeaderContainerID */ _
	/* fieldHeaderOwnerID */ _
	/* fieldHeaderCreationEpoch */ _
	/* fieldHeaderPayloadLength */ _
	/* fieldHeaderPayloadHash */ _
	/* fieldHeaderType */ _
	/* fieldHeaderHomoHash */ _
	/* fieldHeaderSessionToken */ _
	/* fieldHeaderAttributes */ _
	fieldHeaderSplit
	/* fieldHeaderSessionTokenV2 */ _
)

// Protobuf field numbers for split header message.
const (
	_ = iota
	fieldHeaderSplitParent
	fieldHeaderSplitPrevious
	fieldHeaderSplitParentSignature
	fieldHeaderSplitParentHeader
	/* fieldHeaderSplitChildren */ _
	/* fieldHeaderSplitSplitID */ _
	/* fieldHeaderSplitFirst */ _
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

// TODO: docs.
func RestoreParentHeaderLayout(b []byte) (idf, sigf, hdrf iprotobuf.FieldBounds, err error) {
	rootHdrf, err := iprotobuf.SeekBytesField(b, fieldObjectHeader)
	if err != nil {
		err = iprotobuf.WrapSeekFieldError(fieldObjectHeader, protowire.BytesType, err)
		return
	}

	idf.From = -1
	sigf.From = -1
	hdrf.From = -1

	if rootHdrf.From < 0 {
		return
	}

	splitf, err := iprotobuf.SeekBytesField(b[rootHdrf.ValueFrom:rootHdrf.To], fieldHeaderSplit)
	if err != nil {
		err = iprotobuf.WrapSeekFieldError(fieldHeaderSplit, protowire.BytesType, err)
		return
	}

	if splitf.From < 0 {
		return
	}

	b = b[:rootHdrf.ValueFrom+splitf.To]
	off := rootHdrf.ValueFrom + splitf.ValueFrom
	var prevNum protowire.Number
loop:
	for {
		num, typ, tagLn := protowire.ConsumeTag(b[off:])
		if err = protowire.ParseError(tagLn); err != nil {
			err = iprotobuf.WrapParseFieldTagError(err)
			return
		}

		if num < prevNum {
			err = iprotobuf.NewUnorderedFieldsError(prevNum, num)
			return
		}
		prevNum = num

		switch num {
		case fieldHeaderSplitParent:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParent, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			idf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParent, protowire.BytesType, err)
				return
			}

			off = idf.To
		case fieldHeaderSplitPrevious:
			ln := protowire.ConsumeFieldValue(num, typ, b[off+tagLn:])
			if err = protowire.ParseError(ln); err != nil {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitPrevious, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			off += tagLn + ln
		case fieldHeaderSplitParentSignature:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParentSignature, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			sigf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParentSignature, protowire.BytesType, err)
				return
			}

			off = sigf.To
		case fieldHeaderSplitParentHeader:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParentHeader, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			hdrf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldHeaderSplitParentHeader, protowire.BytesType, err)
				return
			}

			break loop
		default:
			break loop
		}

		if off == len(b) {
			break
		}
	}

	return
}

// TODO: docs.
func RestoreLayoutWithCutPayload(b []byte) (idf, sigf, hdrf iprotobuf.FieldBounds, err error) {
	idf.From = -1
	sigf.From = -1
	hdrf.From = -1

	var off int
	var prevNum protowire.Number
loop:
	for {
		num, typ, tagLn := protowire.ConsumeTag(b[off:])
		if err = protowire.ParseError(tagLn); err != nil {
			err = iprotobuf.WrapParseFieldTagError(err)
			return
		}

		if num < prevNum {
			err = iprotobuf.NewUnorderedFieldsError(prevNum, num)
			return
		}
		prevNum = num

		switch num {
		case fieldObjectID:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldObjectID, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			idf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldObjectID, protowire.BytesType, err)
				return
			}

			off = idf.To
		case fieldObjectSignature:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldObjectSignature, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			sigf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldObjectSignature, protowire.BytesType, err)
				return
			}

			off = sigf.To
		case fieldObjectHeader:
			if typ != protowire.BytesType {
				err = iprotobuf.WrapParseFieldError(fieldObjectHeader, protowire.BytesType, iprotobuf.NewWrongFieldTypeError(typ))
				return
			}

			hdrf, err = iprotobuf.ParseBytesFieldBounds(b, off, tagLn)
			if err != nil {
				err = iprotobuf.WrapParseFieldError(fieldObjectHeader, protowire.BytesType, err)
				return
			}

			break loop
		case fieldObjectPayload:
			if _, n := protowire.ConsumeVarint(b[off+tagLn:]); n < 0 {
				err = iprotobuf.WrapParseFieldError(fieldObjectPayload, protowire.BytesType, protowire.ParseError(n))
				return
			}

			break loop
		default:
			break loop
		}

		if off == len(b) {
			break
		}
	}

	return
}
