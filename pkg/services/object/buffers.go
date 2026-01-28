package object

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

const maxObjectHeaderVarintLen = 3 // 16K

const (
	tagField1Bytes = 10
	tagField2Bytes = 18
)

const (
	maxHeadResponseBodyVarintLen  = maxObjectHeaderVarintLen
	maxHeaderOffsetInHeadResponse = 1 + maxHeadResponseBodyVarintLen + 1 + maxObjectHeaderVarintLen // 1 for tagField1Bytes
)

type headResponseBuffer struct {
	mem.SliceBuffer
	len  int
	refs atomic.Int32
}

func (x *headResponseBuffer) finalize(hdrLen int) error {
	buf := x.SliceBuffer

	hdrBuf := buf[maxHeaderOffsetInHeadResponse:]

	// TODO: cover missing fields
	off, err := iprotobuf.SeekFieldByNumber(hdrBuf, 2) // signature
	if err != nil {
		return fmt.Errorf("seek signature field: %w", err)
	}
	hdrBuf = hdrBuf[off:]
	hdrLen -= off

	hdrOff, err := iprotobuf.SeekFieldByNumber(hdrBuf, 3) // signature
	if err != nil {
		return fmt.Errorf("seek header field: %w", err)
	}

	hdrBuf[0] = tagField2Bytes
	hdrBuf[hdrOff] = tagField1Bytes

	hdrFldLen := 1 + protowire.SizeBytes(hdrLen)

	hdrOff = 1 + binary.PutUvarint(buf[1:], uint64(hdrFldLen))
	buf[hdrOff] = tagField1Bytes
	hdrOff += 1 + binary.PutUvarint(buf[hdrOff+1:], uint64(hdrLen))

	copy(buf[hdrOff:], hdrBuf[:hdrLen])

	x.len = hdrOff + hdrLen
	x.refs.Store(1)

	return nil
}

func (x *headResponseBuffer) Len() int {
	return x.len
}

func (x *headResponseBuffer) Ref() {
	if x.refs.Add(1) <= 1 {
		panic("ref of freed bufer")
	}
}

func (x *headResponseBuffer) Free() {
	switch refs := x.refs.Add(-1); {
	case refs > 0:
	case refs == 0:
		headResponseBufferPool.Put(x)
	default:
		panic("free of freed buffer")
	}
}

var headResponseBufferPool = &sync.Pool{
	New: func() any {
		b := make(mem.SliceBuffer, maxHeaderOffsetInHeadResponse+object.MaxHeaderLen)
		b[0] = tagField1Bytes
		return &headResponseBuffer{
			SliceBuffer: b,
		}
	},
}

func getHeadResponseBuffer() (*headResponseBuffer, []byte) {
	item := headResponseBufferPool.Get().(*headResponseBuffer)
	return item, item.SliceBuffer[maxHeaderOffsetInHeadResponse:]
}
