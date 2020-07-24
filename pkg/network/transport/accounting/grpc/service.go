package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/accounting"
	"github.com/nspcc-dev/neofs-api-go/decimal"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	libgrpc "github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// Service is an interface of the server of Accounting service.
	Service interface {
		grpc.Service
		accounting.AccountingServer
	}

	// ContractClient represents the client of Balance contract.
	//
	// It is a type alias of the pointer to
	// github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper.Wrapper.
	ContractClient = *wrapper.Wrapper

	// Params groups the parameters of Accounting service server's constructor.
	Params struct {
		ContractClient ContractClient
	}

	accService struct {
		contractClient ContractClient
	}
)

var requestVerifyFunc = libgrpc.VerifyRequestWithSignatures

// New is an Accounting service server's constructor.
//
// If Balance contract client is nil,
// wrapper.ErrNilWrapper is returned.
func New(p Params) (Service, error) {
	if p.ContractClient == nil {
		return nil, wrapper.ErrNilWrapper
	}

	return &accService{
		contractClient: p.ContractClient,
	}, nil
}

func (accService) Name() string { return "AccountingService" }

func (s accService) Register(g *grpc.Server) { accounting.RegisterAccountingServer(g, s) }

func (s accService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// get the amount of funds in client's account
	fundsAmount, err := s.contractClient.BalanceOf(req.GetOwnerID())
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// get decimals precision of currency transactions

	// TODO: Reconsider the approach of getting decimals.
	//
	//  Decimals value does not seem to be frequently changing.
	// In this case service can work in static decimals mode and
	// the value can be received once to facilitate call flow.
	//
	// In a true dynamic value installation it is advisable to get
	// a balance with decimals through a single call. Variations:
	//  - add decimal value stack parameter of balanceOf method;
	//  - create a new method entitled smth like balanceWithDecimals.
	decimals, err := s.contractClient.Decimals()
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	res := new(accounting.BalanceResponse)
	res.Balance = decimal.NewWithPrecision(
		fundsAmount,
		decimals,
	)

	return res, nil
}
