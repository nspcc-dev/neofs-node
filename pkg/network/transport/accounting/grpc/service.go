package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	accountingGRPC "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	accountingsvc "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
)

// Server wraps NeoFS API Accounting service and
// provides gRPC Accounting service server interface.
type Server struct {
	srv accountingsvc.Server
}

// New creates, initializes and returns Server instance.
func New(c accountingsvc.Server) *Server {
	return &Server{
		srv: c,
	}
}

// Balance converts gRPC BalanceRequest message and passes it to internal Accounting service.
func (s *Server) Balance(ctx context.Context, req *accountingGRPC.BalanceRequest) (*accountingGRPC.BalanceResponse, error) {
	balReq := new(accounting.BalanceRequest)
	if err := balReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Balance(ctx, balReq)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*accountingGRPC.BalanceResponse), nil
}
