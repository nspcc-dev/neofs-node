package protoscan_test

import (
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"google.golang.org/grpc/mem"
)

func newSingleBufferSlice(buf []byte) iprotobuf.BuffersSlice {
	return iprotobuf.NewBuffersSlice(mem.BufferSlice{mem.SliceBuffer(buf)})
}

func byteByByteSlice(msg []byte) mem.BufferSlice {
	buffers := make(mem.BufferSlice, 0, len(msg)*2)
	for i := range msg {
		// add intermediate empty buffers just to make sure they cause no problems
		buffers = append(buffers, mem.SliceBuffer{}, mem.SliceBuffer([]byte{msg[i]}))
	}

	return buffers
}
