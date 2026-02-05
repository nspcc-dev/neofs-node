package protobuf

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// TODO: docs.
type BufferedCodec struct{}

// TODO: docs.
func (BufferedCodec) Marshal(msg any) (mem.BufferSlice, error) {
	if bs, ok := msg.(mem.Buffer); ok {
		return mem.BufferSlice{bs}, nil
	}
	return encoding.GetCodecV2(proto.Name).Marshal(msg)
}

// TODO: docs.
func (BufferedCodec) Unmarshal(data mem.BufferSlice, msg any) error {
	return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
}

// TODO: docs.
func (BufferedCodec) Name() string {
	// may be any non-empty, conflicts are unlikely to arise
	return "neofs_custom_buffers"
}
