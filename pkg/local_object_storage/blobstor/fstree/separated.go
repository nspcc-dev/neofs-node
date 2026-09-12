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

	separatedVersion = 1

	separatedHeaderLenOff  = 2
	separatedPayloadLenOff = separatedHeaderLenOff + 8
	separatedDataOff       = separatedPayloadLenOff + 8
)

// separateObject stores the canonical object prefix through the payload length
// field separately from the raw payload. Objects that cannot be safely split
// retain their original form.
func separateObject(data []byte) []byte {
	off, tagLen, typ, err := iprotobuf.SeekFieldByNumber(data, protoobject.FieldObjectPayload)
	if err != nil || off < 0 || typ != protowire.BytesType {
		return data
	}

	payloadLen, lenLen, err := iprotobuf.ParseVarint(data[off+tagLen:])
	if err != nil {
		return data
	}
	payloadOff := off + tagLen + lenLen
	if payloadOff < off || payloadOff > len(data) || payloadLen != uint64(len(data)-payloadOff) {
		return data
	}

	res := make([]byte, separatedDataOff+payloadOff+int(payloadLen))
	copy(res, separatedObjectPrefix(uint64(payloadOff), payloadLen))
	copy(res[separatedDataOff:], data[:payloadOff])
	copy(res[separatedDataOff+payloadOff:], data[payloadOff:])
	return res
}

func separatedObjectPrefix(headerLen, payloadLen uint64) []byte {
	prefix := make([]byte, separatedDataOff)
	prefix[0] = separatedPrefix
	prefix[1] = separatedVersion
	binary.BigEndian.PutUint64(prefix[separatedHeaderLenOff:], headerLen)
	binary.BigEndian.PutUint64(prefix[separatedPayloadLenOff:], payloadLen)
	return prefix
}

// parseSeparatedPrefix returns lengths of the separately stored canonical
// header prefix and payload. Zero values mean that data is not a separated
// object.
func parseSeparatedPrefix(data []byte) (uint64, uint64) {
	if len(data) < separatedDataOff || data[0] != separatedPrefix || data[1] != separatedVersion {
		return 0, 0
	}
	headerLen := binary.BigEndian.Uint64(data[separatedHeaderLenOff:separatedPayloadLenOff])
	payloadLen := binary.BigEndian.Uint64(data[separatedPayloadLenOff:separatedDataOff])
	if headerLen == 0 {
		return 0, 0
	}
	return headerLen, payloadLen
}

// preprocessSeparatedObject returns the canonical object prefix and a stream
// positioned at the payload remainder. It takes ownership of f on success and
// on error.
func preprocessSeparatedObject(f io.ReadSeekCloser, initial []byte, headerLen, payloadLen uint64) ([]byte, io.ReadSeekCloser, error) {
	if headerLen > math.MaxInt || payloadLen > math.MaxInt64 {
		return nil, f, fmt.Errorf("separated object length overflows: header %d, payload %d", headerLen, payloadLen)
	}
	headerEnd := separatedDataOff + int(headerLen)
	if headerEnd < separatedDataOff || headerEnd > cap(initial) {
		return nil, f, fmt.Errorf("separated object header length overflows buffer: %d", headerLen)
	}

	bufferedLen := len(initial)
	if bufferedLen < headerEnd {
		initial = initial[:headerEnd]
		if _, err := io.ReadFull(f, initial[bufferedLen:]); err != nil {
			return nil, f, fmt.Errorf("read separated object header: %w", err)
		}
		bufferedLen = headerEnd
	}

	payloadPrefixLen := bufferedLen - headerEnd
	if uint64(payloadPrefixLen) > payloadLen {
		return nil, f, fmt.Errorf("invalid separated object payload length: %d", payloadLen)
	}

	// Reuse the caller's buffer: the stored header is already a canonical
	// prefix, so only remove the storage prefix and compact any pre-read payload.
	copy(initial, initial[separatedDataOff:headerEnd])
	copy(initial[int(headerLen):], initial[headerEnd:bufferedLen])
	initial = initial[:int(headerLen)+payloadPrefixLen]
	return initial, &limitedFileReader{ReadSeekCloser: f, limit: int64(payloadLen) - int64(payloadPrefixLen)}, nil
}

// restoreSeparatedObject converts a separated object into canonical NeoFS
// binary form.
func restoreSeparatedObject(data []byte) ([]byte, bool, error) {
	headerLen, payloadLen := parseSeparatedPrefix(data)
	if headerLen == 0 {
		return data, false, nil
	}
	dataLen := uint64(len(data) - separatedDataOff)
	if headerLen > math.MaxInt || headerLen > dataLen || payloadLen != dataLen-headerLen {
		return nil, true, fmt.Errorf("invalid separated object lengths: header %d, payload %d, data %d", headerLen, payloadLen, len(data))
	}

	copy(data, data[separatedDataOff:])
	return data[:len(data)-separatedDataOff], true, nil
}
