package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	protonetmap "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
)

// Server is an interface of the NeoFS API Netmap service server.
type Server interface {
	LocalNodeInfo(context.Context, *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error)
	NetworkInfo(context.Context, *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error)
	Snapshot(context.Context, *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error)
}

type server struct {
	protonetmap.UnimplementedNetmapServiceServer
	srv Server
}

// New returns protonetmap.NetmapServiceServer based on the Server.
func New(c Server) protonetmap.NetmapServiceServer {
	return &server{
		srv: c,
	}
}

// LocalNodeInfo converts gRPC request message and passes it to internal netmap service.
func (s server) LocalNodeInfo(
	ctx context.Context,
	req *protonetmap.LocalNodeInfoRequest) (*protonetmap.LocalNodeInfoResponse, error) {
	nodeInfoReq := new(netmap.LocalNodeInfoRequest)
	if err := nodeInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.LocalNodeInfo(ctx, nodeInfoReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protonetmap.LocalNodeInfoResponse), nil
}

// NetworkInfo converts gRPC request message and passes it to internal netmap service.
func (s *server) NetworkInfo(ctx context.Context, req *protonetmap.NetworkInfoRequest) (*protonetmap.NetworkInfoResponse, error) {
	netInfoReq := new(netmap.NetworkInfoRequest)
	if err := netInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.NetworkInfo(ctx, netInfoReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protonetmap.NetworkInfoResponse), nil
}

// NetmapSnapshot converts gRPC request message and passes it to internal netmap service.
func (s *server) NetmapSnapshot(ctx context.Context, req *protonetmap.NetmapSnapshotRequest) (*protonetmap.NetmapSnapshotResponse, error) {
	snapshotReq := new(netmap.SnapshotRequest)
	if err := snapshotReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Snapshot(ctx, snapshotReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protonetmap.NetmapSnapshotResponse), nil
}
