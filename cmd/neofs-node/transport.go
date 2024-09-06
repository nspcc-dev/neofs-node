package main

import (
	"context"
	"fmt"

	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/common"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/message"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

type transport struct {
	clients *coreClientConstructor
}

// SendReplicationRequestToNode connects to described node and sends prepared
// replication request message to it.
func (x *transport) SendReplicationRequestToNode(ctx context.Context, req []byte, node coreclient.NodeInfo) (*neofscrypto.Signature, error) {
	c, err := x.clients.Get(node)
	if err != nil {
		return nil, fmt.Errorf("connect to remote node: %w", err)
	}

	var resp replicateResponse
	err = c.ExecRaw(func(c *rawclient.Client) error {
		// this will be changed during NeoFS API Go deprecation. Code most likely be
		// placed in SDK
		m := common.CallMethodInfo{Service: "neo.fs.v2.object.ObjectService", Name: "Replicate"}
		err = rawclient.SendUnary(c, m, rawclient.BinaryMessage(req), &resp,
			rawclient.WithContext(ctx), rawclient.AllowBinarySendingOnly())
		if err != nil {
			return fmt.Errorf("API transport (service=%s,op=%s): %w", m.Service, m.Name, err)
		}
		return resp.err
	})
	return resp.sig, err
}

type replicateResponse struct {
	sig *neofscrypto.Signature
	err error
}

func (x replicateResponse) ToGRPCMessage() grpc.Message { return new(objectGRPC.ReplicateResponse) }

func (x *replicateResponse) FromGRPCMessage(gm grpc.Message) error {
	m, ok := gm.(*objectGRPC.ReplicateResponse)
	if !ok {
		return message.NewUnexpectedMessageType(gm, m)
	}

	var st *status.Status
	if mst := m.GetStatus(); mst != nil {
		st = new(status.Status)
		err := st.FromGRPCMessage(mst)
		if err != nil {
			return fmt.Errorf("decode response status: %w", err)
		}
	}

	x.err = apistatus.ErrorFromV2(st)
	if x.err != nil {
		return nil
	}

	sig := m.GetObjectSignature()
	if sig == nil {
		return nil
	}

	sigV2 := new(refs.Signature)
	err := sigV2.Unmarshal(sig)
	if err != nil {
		return fmt.Errorf("decoding signature from proto message: %w", err)
	}

	x.sig = new(neofscrypto.Signature)
	err = x.sig.ReadFromV2(*sigV2)
	if err != nil {
		return fmt.Errorf("invalid signature: %w", err)
	}

	return nil
}
