package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	accountingGRPC "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
)

// Server wraps NeoFS API Accounting service and
// provides gRPC Accounting service server interface.
type Server struct {
	srv accounting.Service
}

// New creates, initializes and returns Server instance.
func New(c accounting.Service) *Server {
	return &Server{
		srv: c,
	}
}

// Balance converts gRPC BalanceRequest message and passes it to internal Accounting service.
func (s *Server) Balance(ctx context.Context, req *accountingGRPC.BalanceRequest) (*accountingGRPC.BalanceResponse, error) {
	resp, err := s.srv.Balance(ctx, accounting.BalanceRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return accounting.BalanceResponseToGRPCMessage(resp), nil
}
