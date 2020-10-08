package grpc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
)

// Server wraps NeoFS API Netmap service and
// provides gRPC Netmap service server interface.
type Server struct {
	srv netmap.Service
}

// New creates, initializes and returns Server instance.
func New(c netmap.Service) *Server {
	return &Server{
		srv: c,
	}
}

// LocalNodeInfo converts gRPC request message and passes it to internal netmap service.
func (s *Server) LocalNodeInfo(
	ctx context.Context,
	req *netmapGRPC.LocalNodeInfoRequest) (*netmapGRPC.LocalNodeInfoResponse, error) {

	resp, err := s.srv.LocalNodeInfo(ctx, netmap.LocalNodeInfoRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return netmap.LocalNodeInfoResponseToGRPCMessage(resp), nil
}
