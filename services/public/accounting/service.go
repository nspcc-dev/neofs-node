package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/accounting"
	"github.com/nspcc-dev/neofs-api-go/decimal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// Service is an interface of the server of Accounting service.
	Service interface {
		grpc.Service
		accounting.AccountingServer
	}

	// Params groups the parameters of Accounting service server's constructor.
	Params struct {
		MorphBalanceContract implementations.MorphBalanceContract
	}

	accService struct {
		balanceContract implementations.MorphBalanceContract
	}
)

var requestVerifyFunc = core.VerifyRequestWithSignatures

// New is an Accounting service server's constructor.
func New(p Params) (Service, error) {
	return &accService{
		balanceContract: p.MorphBalanceContract,
	}, nil
}

func (accService) Name() string { return "AccountingService" }

func (s accService) Register(g *grpc.Server) { accounting.RegisterAccountingServer(g, s) }

func (s accService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	// verify request structure
	if err := requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// prepare balanceOf parameters
	p := implementations.BalanceOfParams{}
	p.SetOwnerID(req.GetOwnerID())

	// get balance of
	bRes, err := s.balanceContract.BalanceOf(p)
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	// get decimals

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
	decRes, err := s.balanceContract.Decimals(implementations.DecimalsParams{})
	if err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	res := new(accounting.BalanceResponse)
	res.Balance = decimal.NewWithPrecision(
		bRes.Amount(),
		uint32(decRes.Decimals()),
	)

	return res, nil
}
