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
	if bs, ok := msg.(mem.Buffer); ok {
		return mem.BufferSlice{bs}, nil
	}
	return encoding.GetCodecV2(proto.Name).Marshal(msg)
}

// Unmarshal implements [encoding.CodecV2].
func (BufferedCodec) Unmarshal(data mem.BufferSlice, msg any) error {
	return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
}

// Name implements [encoding.CodecV2].
func (BufferedCodec) Name() string {
	return "neofs_custom_buffers"
}
