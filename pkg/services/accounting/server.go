package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	protoaccounting "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
)

// Server is an interface of the NeoFS API Accounting service server.
type Server interface {
	Balance(context.Context, *accounting.BalanceRequest) (*accounting.BalanceResponse, error)
}

type server struct {
	protoaccounting.UnimplementedAccountingServiceServer
	srv Server
}

// New returns protoaccounting.AccountingServiceServer based on the Server.
func New(c Server) protoaccounting.AccountingServiceServer {
	return &server{
		srv: c,
	}
}

// Balance converts gRPC BalanceRequest message and passes it to internal Accounting service.
func (s *server) Balance(ctx context.Context, req *protoaccounting.BalanceRequest) (*protoaccounting.BalanceResponse, error) {
	balReq := new(accounting.BalanceRequest)
	if err := balReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Balance(ctx, balReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protoaccounting.BalanceResponse), nil
}
