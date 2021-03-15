package grpc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	netmapsvc "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
)

// Server wraps NeoFS API Netmap service and
// provides gRPC Netmap service server interface.
type Server struct {
	srv netmapsvc.Server
}

// New creates, initializes and returns Server instance.
func New(c netmapsvc.Server) *Server {
	return &Server{
		srv: c,
	}
}

// LocalNodeInfo converts gRPC request message and passes it to internal netmap service.
func (s *Server) LocalNodeInfo(
	ctx context.Context,
	req *netmapGRPC.LocalNodeInfoRequest) (*netmapGRPC.LocalNodeInfoResponse, error) {
	nodeInfoReq := new(netmap.LocalNodeInfoRequest)
	if err := nodeInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.LocalNodeInfo(ctx, nodeInfoReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*netmapGRPC.LocalNodeInfoResponse), nil
}

// NetworkInfo converts gRPC request message and passes it to internal netmap service.
func (s *Server) NetworkInfo(ctx context.Context, req *netmapGRPC.NetworkInfoRequest) (*netmapGRPC.NetworkInfoResponse, error) {
	netInfoReq := new(netmap.NetworkInfoRequest)
	if err := netInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.NetworkInfo(ctx, netInfoReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*netmapGRPC.NetworkInfoResponse), nil
}
