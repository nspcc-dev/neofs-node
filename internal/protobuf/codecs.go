package protobuf

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// BuffersCodec is an extension of default gRPC codec supporting buffered
// messages.
type BuffersCodec struct{}

// Marshal implements [encoding.CodecV2].
func (BuffersCodec) Marshal(msg any) (mem.BufferSlice, error) {
	switch v := msg.(type) {
	case mem.BufferSlice:
		return v, nil
	default:
		return encoding.GetCodecV2(proto.Name).Marshal(msg)
	}
}

// Unmarshal implements [encoding.CodecV2].
func (BuffersCodec) Unmarshal(data mem.BufferSlice, msg any) error {
	switch v := msg.(type) {
	case *mem.BufferSlice:
		if data == nil {
			data = mem.BufferSlice{}
		}
		*v = data
		data.Ref()
		return nil
	default:
		return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
	}
}

// Name implements [encoding.CodecV2].
func (BuffersCodec) Name() string {
	// may be any non-empty, conflicts are unlikely to arise
	return "neofs_custom_buffers"
}
