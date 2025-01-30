package main

import (
	"context"
	"errors"
	"fmt"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
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

	var resp protoobject.ReplicateResponse
	conn := c.Conn()
	if conn == nil {
		return nil, errors.New("can't get grpc connection to node")
	}
	// this will be changed during NeoFS API Go deprecation. Code most likely be
	// placed in SDK
	err = conn.Invoke(ctx, protoobject.ObjectService_Replicate_FullMethodName, req, &resp, binaryMessageOnly)
	if err != nil {
		return nil, fmt.Errorf("API transport (op=%s): %w", protoobject.ObjectService_Replicate_FullMethodName, err)
	}

	return replicationResultFromResponse(&resp)
}

// [encoding.Codec] making Marshal to accept and forward []byte messages only.
var binaryMessageOnly = grpc.ForceCodec(protoCodecBinaryRequestOnly{})

type protoCodecBinaryRequestOnly struct{}

func (protoCodecBinaryRequestOnly) Name() string {
	// may be any non-empty, conflicts are unlikely to arise
	return "neofs_binary_sender"
}

func (protoCodecBinaryRequestOnly) Marshal(msg any) ([]byte, error) {
	bMsg, ok := msg.([]byte)
	if ok {
		return bMsg, nil
	}

	return nil, fmt.Errorf("message is not of type %T", bMsg)
}

func (protoCodecBinaryRequestOnly) Unmarshal(raw []byte, msg any) error {
	return encoding.GetCodec(proto.Name).Unmarshal(raw, msg)
}

func replicationResultFromResponse(m *protoobject.ReplicateResponse) ([]byte, error) {
	err := apistatus.ToError(m.GetStatus())
	if err != nil {
		return nil, err
	}

	return m.GetObjectSignature(), nil
}
