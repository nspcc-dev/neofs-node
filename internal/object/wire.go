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

// Protobuf field numbers for object message.
const (
	_ = iota
	fieldObjectID
	fieldObjectSignature
	fieldObjectHeader
	fieldObjectPayload

	FieldHeaderVersion        = 1
	FieldHeaderContainerID    = 2
	FieldHeaderOwnerID        = 3
	FieldHeaderCreationEpoch  = 4
	FieldHeaderPayloadLength  = 5
	FieldHeaderPayloadHash    = 6
	FieldHeaderType           = 7
	FieldHeaderHomoHash       = 8
	FieldHeaderSessionToken   = 9
	FieldHeaderAttributes     = 10
	FieldHeaderSplit          = 11
	FieldHeaderSessionTokenV2 = 12

	FieldHeaderSplitParent          = 1
	FieldHeaderSplitPrevious        = 2
	FieldHeaderSplitParentSignature = 3
	FieldHeaderSplitParentHeader    = 4
	FieldHeaderSplitChildren        = 5
	FieldHeaderSplitSplitID         = 6
	FieldHeaderSplitFirst           = 7
)

var errEmptyData = errors.New("empty data")

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

		if num > fieldObjectHeader {
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
		case fieldObjectID:
			idf = f
		case fieldObjectSignature:
			sigf = f
		case fieldObjectHeader:
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

	rootHdrf, err := iprotobuf.GetLENFieldBounds(buf, fieldObjectHeader)
	if err != nil {
		return idf, sigf, hdrf, err
	}

	if rootHdrf.IsMissing() {
		return idf, sigf, hdrf, nil
	}

	splitf, err := iprotobuf.GetLENFieldBounds(buf[rootHdrf.ValueFrom:rootHdrf.To], FieldHeaderSplit)
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

		if num > FieldHeaderSplitParentHeader {
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
		case FieldHeaderSplitParent:
			idf = f
		case FieldHeaderSplitPrevious:
		case FieldHeaderSplitParentSignature:
			sigf = f
		case FieldHeaderSplitParentHeader:
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
