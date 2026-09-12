package fstree

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	// separatedPrefix marks an object with the header and payload stored
	// independently. It cannot start a valid Object protobuf message.
	separatedPrefix = 0x7e

	separatedVersion = 0

	separatedHeaderLenOff  = 2
	separatedPayloadLenOff = separatedHeaderLenOff + 4
	separatedDataOff       = separatedPayloadLenOff + 4
)

// separateObject stores the object fields preceding the payload separately from
// the payload. Objects that cannot be safely split retain their original form.
func separateObject(data []byte) []byte {
	off, tagLen, typ, err := iprotobuf.SeekFieldByNumber(data, protoobject.FieldObjectPayload)
	if err != nil || off < 0 || typ != protowire.BytesType {
		return data
	}

	payloadLen, lenLen, err := iprotobuf.ParseVarint(data[off+tagLen:])
	if err != nil || payloadLen > uint64(len(data)-off-tagLen-lenLen) {
		return data
	}
	payloadOff := off + tagLen + lenLen
	if payloadLen != uint64(len(data)-payloadOff) || off > math.MaxUint32 || payloadLen > math.MaxUint32 {
		return data
	}

	res := make([]byte, separatedDataOff+len(data))
	res[0] = separatedPrefix
	res[1] = separatedVersion
	binary.BigEndian.PutUint32(res[separatedHeaderLenOff:], uint32(off))
	binary.BigEndian.PutUint32(res[separatedPayloadLenOff:], uint32(payloadLen))
	copy(res[separatedDataOff:], data[:off])
	copy(res[separatedDataOff+off:], data[payloadOff:])
	return res[:separatedDataOff+off+int(payloadLen)]
}

// parseSeparatedPrefix returns lengths of the separately stored header and
// payload. Zero values mean that data is not a separated object.
func parseSeparatedPrefix(data []byte) (uint32, uint32) {
	if len(data) < separatedDataOff || data[0] != separatedPrefix || data[1] != separatedVersion {
		return 0, 0
	}
	headerLen := binary.BigEndian.Uint32(data[separatedHeaderLenOff:separatedPayloadLenOff])
	payloadLen := binary.BigEndian.Uint32(data[separatedPayloadLenOff:separatedDataOff])
	if headerLen == 0 {
		return 0, 0
	}
	return headerLen, payloadLen
}

// restoreSeparatedObject converts a separated object into canonical NeoFS
// binary form.
// preprocessSeparatedObject returns enough canonical object bytes to parse the
// header and a stream positioned at the payload remainder. It takes ownership
// of f on success and on error.
func preprocessSeparatedObject(f io.ReadSeekCloser, initial []byte, headerLen, payloadLen uint32) ([]byte, io.ReadSeekCloser, error) {
	headerEnd := separatedDataOff + int(headerLen)
	if headerEnd < separatedDataOff {
		return nil, f, fmt.Errorf("separated object header length overflows: %d", headerLen)
	}

	header := make([]byte, int(headerLen))
	if len(initial) >= headerEnd {
		copy(header, initial[separatedDataOff:headerEnd])
	} else {
		if len(initial) < separatedDataOff {
			return nil, f, fmt.Errorf("truncated separated object prefix")
		}
		copied := copy(header, initial[separatedDataOff:])
		if _, err := io.ReadFull(f, header[copied:]); err != nil {
			return nil, f, fmt.Errorf("read separated object header: %w", err)
		}
	}

	var payloadPrefix []byte
	if len(initial) > headerEnd {
		payloadPrefix = initial[headerEnd:]
	}
	if uint64(len(payloadPrefix)) > uint64(payloadLen) {
		return nil, f, fmt.Errorf("invalid separated object payload length: %d", payloadLen)
	}

	res := make([]byte, 0, len(header)+protowire.SizeTag(protoobject.FieldObjectPayload)+protowire.SizeVarint(uint64(payloadLen))+len(payloadPrefix))
	res = append(res, header...)
	res = protowire.AppendTag(res, protoobject.FieldObjectPayload, protowire.BytesType)
	res = protowire.AppendVarint(res, uint64(payloadLen))
	res = append(res, payloadPrefix...)
	return res, &limitedFileReader{ReadSeekCloser: f, limit: int64(payloadLen) - int64(len(payloadPrefix))}, nil
}

func restoreSeparatedObject(data []byte) ([]byte, bool, error) {
	headerLen, payloadLen := parseSeparatedPrefix(data)
	if headerLen == 0 {
		return data, false, nil
	}
	if uint64(separatedDataOff)+uint64(headerLen)+uint64(payloadLen) != uint64(len(data)) {
		return nil, true, fmt.Errorf("invalid separated object lengths: header %d, payload %d, data %d", headerLen, payloadLen, len(data))
	}

	res := make([]byte, 0, len(data)-separatedDataOff+protowire.SizeTag(protoobject.FieldObjectPayload)+protowire.SizeVarint(uint64(payloadLen)))
	res = append(res, data[separatedDataOff:separatedDataOff+int(headerLen)]...)
	res = protowire.AppendTag(res, protoobject.FieldObjectPayload, protowire.BytesType)
	res = protowire.AppendVarint(res, uint64(payloadLen))
	res = append(res, data[separatedDataOff+int(headerLen):]...)
	return res, true, nil
}
