package main

import (
	"context"
	"fmt"

	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/common"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/message"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

type api struct {
	clients *coreClientConstructor
}

type apiAdapter api

// TODO: docs.
func (x *api) ReplicateToNode(ctx context.Context, req []byte, node netmap.NodeInfo) error {
	var n coreclient.NodeInfo
	err := coreclient.NodeInfoFromRawNetmapElement(&n, netmapcore.Node(node))
	if err != nil {
		return fmt.Errorf("parse info about storage node from the network map: %w", err)
	}

	return (*apiAdapter)(x).ReplicateToNode(ctx, req, n)
}

// TODO: docs.
func (x *apiAdapter) ReplicateToNode(ctx context.Context, req []byte, node coreclient.NodeInfo) error {
	mc, err := x.clients.Get(node)
	if err != nil {
		return fmt.Errorf("connect to node: %w", err)
	}

	return mc.ExecRaw(func(c *rawclient.Client) error {
		m := common.CallMethodInfo{Service: "neo.fs.v2.object.ObjectService", Name: "Replicate"}
		var resp replicateResponse
		err = rawclient.SendUnary(c, m, rawclient.BinaryMessage(req), &resp,
			rawclient.WithContext(ctx), rawclient.AllowBinarySendingOnly())
		if err != nil {
			return fmt.Errorf("API transport (service=%s,op=%s): %w", m.Service, m.Name, err)
		}
		return resp.err
	})
}

type replicateResponse struct{ err error }

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

	return nil
}
