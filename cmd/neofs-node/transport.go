package main

import (
	"context"
	"fmt"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
)

type transport struct {
	clients *coreClientConstructor
}

// SendReplicationRequestToNode connects to described node and sends prepared
// replication request message to it.
func (x *transport) SendReplicationRequestToNode(ctx context.Context, req []byte, node coreclient.NodeInfo) ([]byte, error) {
	c, err := x.clients.Get(node)
	if err != nil {
		return nil, fmt.Errorf("connect to remote node: %w", err)
	}

	var res []byte
	return res, c.ForEachGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		// this will be changed during NeoFS API Go deprecation. Code most likely be
		// placed in SDK
		var resp protoobject.ReplicateResponse
		err := conn.Invoke(ctx, protoobject.ObjectService_Replicate_FullMethodName, req, &resp, binaryMessageOnly)
		if err != nil {
			return fmt.Errorf("API transport (op=%s): %w", protoobject.ObjectService_Replicate_FullMethodName, err)
		}
		res, err = replicationResultFromResponse(&resp)
		return err
	})
}

// [encoding.Codec] making Marshal to accept and forward []byte messages only.
var binaryMessageOnly = grpc.ForceCodecV2(protoCodecBinaryRequestOnly{})

type protoCodecBinaryRequestOnly struct{}

func (protoCodecBinaryRequestOnly) Name() string {
	// may be any non-empty, conflicts are unlikely to arise
	return "neofs_binary_sender"
}

func (protoCodecBinaryRequestOnly) Marshal(msg any) (mem.BufferSlice, error) {
	bMsg, ok := msg.([]byte)
	if ok {
		return mem.BufferSlice{mem.NewBuffer(&bMsg, mem.DefaultBufferPool())}, nil
	}

	return nil, fmt.Errorf("message is not of type %T", bMsg)
}

func (protoCodecBinaryRequestOnly) Unmarshal(data mem.BufferSlice, msg any) error {
	return encoding.GetCodecV2(proto.Name).Unmarshal(data, msg)
}

func replicationResultFromResponse(m *protoobject.ReplicateResponse) ([]byte, error) {
	err := apistatus.ToError(m.GetStatus())
	if err != nil {
		return nil, err
	}

	return m.GetObjectSignature(), nil
}
