package protobuf

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

// BufferedCodec is [encoding.CodecV2] extending default one provided by [proto]
// package with custom buffers' support.
type BufferedCodec struct{}

// Marshal implements [encoding.CodecV2].
func (BufferedCodec) Marshal(msg any) (mem.BufferSlice, error) {
	switch v := msg.(type) {
	case mem.BufferSlice:
		return v, nil
	case mem.Buffer:
		return mem.BufferSlice{v}, nil
	default:
		return encoding.GetCodecV2(proto.Name).Marshal(msg)
	}
}

// Unmarshal implements [encoding.CodecV2].
func (BufferedCodec) Unmarshal(data mem.BufferSlice, msg any) error {
	switch v := msg.(type) {
	case *mem.BufferSlice:
		data.Ref()
		*v = data
		return nil
	default:
		return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
	}
}

// Name implements [encoding.CodecV2].
func (BufferedCodec) Name() string {
	return "neofs_custom_buffers"
}
