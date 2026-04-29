package object

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// MaxHeaderVarintLen is varint len of [object.MaxHeaderLen].
const MaxHeaderVarintLen = 3

// NonPayloadFieldsBufferLength is a length of the buffer sufficient to read an
// object of maximum size without taking into account the payload. Its value is
// a multiple of 4K.
const NonPayloadFieldsBufferLength = 20 << 10

var errEmptyData = errors.New("empty data")

// WriteWithoutPayload writes the object header to the given writer without the payload.
func WriteWithoutPayload(w io.Writer, obj object.Object) error {
	header := obj.CutPayload().Marshal()
	if obj.PayloadSize() != 0 {
		header = protowire.AppendTag(header, protoobject.FieldObjectPayload, protowire.BytesType)
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
		return nil, nil, errEmptyData
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

		if num == protoobject.FieldObjectPayload {
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
		case protoobject.FieldObjectID:
			obj.ObjectId = new(refs.ObjectID)
			if err := proto.Unmarshal(val, obj.ObjectId); err != nil {
				return nil, nil, fmt.Errorf("unmarshal object ID: %w", err)
			}
		case protoobject.FieldObjectSignature:
			obj.Signature = new(refs.Signature)
			if err := proto.Unmarshal(val, obj.Signature); err != nil {
				return nil, nil, fmt.Errorf("unmarshal object signature: %w", err)
			}
		case protoobject.FieldObjectHeader:
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

// GetNonPayloadFieldBounds seeks ID, signature and header in object message and
// parses their boundaries.
//
// If buf is empty, GetNonPayloadFieldBounds returns an error.
//
// If any field is missing, no error is returned.
//
// Message should have ascending field order, otherwise error returns.
func GetNonPayloadFieldBounds(buf []byte) (iprotobuf.FieldBounds, iprotobuf.FieldBounds, iprotobuf.FieldBounds, error) {
	var idf, sigf, hdrf iprotobuf.FieldBounds
	if len(buf) == 0 {
		return idf, sigf, hdrf, errEmptyData
	}

	var off int
	var prevNum protowire.Number
loop:
	for {
		num, typ, n, err := iprotobuf.ParseTag(buf[off:])
		if err != nil {
			return idf, sigf, hdrf, err
		}

		if num > protoobject.FieldObjectHeader {
			break
		}
		if num < prevNum {
			return idf, sigf, hdrf, iprotobuf.NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return idf, sigf, hdrf, iprotobuf.NewRepeatedFieldError(num)
		}
		prevNum = num

		f, err := iprotobuf.ParseLENFieldBounds(buf, off, n, num, typ)
		if err != nil {
			return idf, sigf, hdrf, err
		}

		switch num {
		case protoobject.FieldObjectID:
			idf = f
		case protoobject.FieldObjectSignature:
			sigf = f
		case protoobject.FieldObjectHeader:
			hdrf = f
			break loop
		default:
			panic("unreachable with num " + strconv.Itoa(int(num)))
		}

		off = f.To

		if off == len(buf) {
			break
		}
	}

	return idf, sigf, hdrf, nil
}

// GetParentNonPayloadFieldBounds seeks parent's ID, signature and header in child
// object message and parses their boundaries.
//
// If buf is empty, GetParentNonPayloadFieldBounds returns an error.
//
// If any field is missing, no error is returned.
//
// Message should have ascending field order, otherwise error returns.
func GetParentNonPayloadFieldBounds(buf []byte) (iprotobuf.FieldBounds, iprotobuf.FieldBounds, iprotobuf.FieldBounds, error) {
	var idf, sigf, hdrf iprotobuf.FieldBounds
	if len(buf) == 0 {
		return idf, sigf, hdrf, errEmptyData
	}

	rootHdrf, err := iprotobuf.GetLENFieldBounds(buf, protoobject.FieldObjectHeader)
	if err != nil {
		return idf, sigf, hdrf, err
	}

	if rootHdrf.IsMissing() {
		return idf, sigf, hdrf, nil
	}

	splitf, err := iprotobuf.GetLENFieldBounds(buf[rootHdrf.ValueFrom:rootHdrf.To], protoobject.FieldHeaderSplit)
	if err != nil {
		return idf, sigf, hdrf, err
	}

	if splitf.IsMissing() {
		return idf, sigf, hdrf, nil
	}

	buf = buf[:rootHdrf.ValueFrom+splitf.To]
	off := rootHdrf.ValueFrom + splitf.ValueFrom
	var prevNum protowire.Number
loop:
	for {
		num, typ, n, err := iprotobuf.ParseTag(buf[off:])
		if err != nil {
			return idf, sigf, hdrf, err
		}

		if num > protoobject.FieldHeaderSplitParentHeader {
			break
		}
		if num < prevNum {
			return idf, sigf, hdrf, iprotobuf.NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return idf, sigf, hdrf, iprotobuf.NewRepeatedFieldError(num)
		}
		prevNum = num

		f, err := iprotobuf.ParseLENFieldBounds(buf, off, n, num, typ)
		if err != nil {
			return idf, sigf, hdrf, err
		}

		switch num {
		case protoobject.FieldHeaderSplitParent:
			idf = f
		case protoobject.FieldHeaderSplitPrevious:
		case protoobject.FieldHeaderSplitParentSignature:
			sigf = f
		case protoobject.FieldHeaderSplitParentHeader:
			hdrf = f
			break loop
		default:
			panic("unreachable with num " + strconv.Itoa(int(num)))
		}

		off = f.To

		if off == len(buf) {
			break
		}
	}

	return idf, sigf, hdrf, nil
}

// GetPayloadLengthAndFieldOffset reads payload length header and seeks payload
// field in buf. If payload field is missing, negative offset returns.
// Otherwise, the offset points to protobuf LV.
//
// If any field is missing, no error is returned.
//
// Message should have ascending field order, otherwise error returns.
func GetPayloadLengthAndFieldOffset(buf []byte) (uint64, int, error) {
	// TODO: traverse buffer at once
	hf, err := iprotobuf.GetLENFieldBounds(buf, protoobject.FieldObjectHeader)
	if err != nil {
		return 0, 0, fmt.Errorf("seek header field: %w", err)
	}

	var pldLen uint64
	if !hf.IsMissing() {
		pldLen, err = iprotobuf.GetUint64Field(buf[hf.ValueFrom:hf.To], protoobject.FieldHeaderPayloadLength)
		if err != nil {
			return 0, 0, fmt.Errorf("seek payload length field in header: %w", err)
		}
	}

	off, tagLn, typ, err := iprotobuf.SeekFieldByNumber(buf, protoobject.FieldObjectPayload)
	if err != nil {
		return 0, 0, fmt.Errorf("seek payload field: %w", err)
	}
	if off < 0 {
		return pldLen, off, nil
	}
	if typ != protowire.BytesType {
		return 0, 0, fmt.Errorf("wrong payload field type: expected %d, got %d", protowire.BytesType, typ)
	}

	return pldLen, off + tagLn, nil
}
