package basic

import (
	"math/big"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"go.uber.org/zap"
)

type (
	EstimationFetcher interface {
		Estimations(uint64) ([]*wrapper.Estimations, error)
	}

	RateFetcher interface {
		BasicRate() (uint64, error)
	}

	// BalanceFetcher uses NEP-17 compatible balance contract
	BalanceFetcher interface {
		Balance(id *owner.ID) (*big.Int, error)
	}

	IncomeSettlementContext struct {
		mu sync.Mutex // lock to prevent collection and distribution in the same time

		log   *zap.Logger
		epoch uint64

		rate        RateFetcher
		estimations EstimationFetcher
		balances    BalanceFetcher
		container   common.ContainerStorage
		placement   common.PlacementCalculator
		exchange    common.Exchanger

		txTable   *common.TransferTable
		bankOwner *owner.ID
	}

	IncomeSettlementContextPrms struct {
		Log         *zap.Logger
		Epoch       uint64
		Rate        RateFetcher
		Estimations EstimationFetcher
		Balances    BalanceFetcher
		Container   common.ContainerStorage
		Placement   common.PlacementCalculator
		Exchange    common.Exchanger
	}
)

func NewIncomeSettlementContext(p *IncomeSettlementContextPrms) (*IncomeSettlementContext, error) {
	bankingAccount, err := bankOwnerID()
	if err != nil {
		return nil, err // should never happen
	}

	return &IncomeSettlementContext{
		log:         p.Log,
		epoch:       p.Epoch,
		rate:        p.Rate,
		estimations: p.Estimations,
		balances:    p.Balances,
		container:   p.Container,
		placement:   p.Placement,
		exchange:    p.Exchange,
		txTable:     common.NewTransferTable(),
		bankOwner:   bankingAccount,
	}, nil
}

func bankOwnerID() (*owner.ID, error) {
	u := util.Uint160{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // todo: define const
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	d, err := base58.Decode(address.Uint160ToString(u))
	if err != nil {
		return nil, err
	}

	var w owner.NEO3Wallet
	copy(w[:], d)

	o := owner.NewID()
	o.SetNeo3Wallet(&w)

	return o, nil
}
