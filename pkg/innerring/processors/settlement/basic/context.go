package basic

import (
	"math/big"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"go.uber.org/zap"
)

type (
	EstimationFetcher interface {
		Estimations(uint64) ([]*container.Estimations, error)
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
		accounts    common.AccountStorage

		bankOwner *owner.ID

		// this table is not thread safe, make sure you use it with mu.Lock()
		distributeTable *NodeSizeTable
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
		Accounts    common.AccountStorage
	}
)

func NewIncomeSettlementContext(p *IncomeSettlementContextPrms) (*IncomeSettlementContext, error) {
	bankingAccount := owner.NewID()
	bankingAccount.SetScriptHash(util.Uint160{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})

	return &IncomeSettlementContext{
		log:             p.Log,
		epoch:           p.Epoch,
		rate:            p.Rate,
		estimations:     p.Estimations,
		balances:        p.Balances,
		container:       p.Container,
		placement:       p.Placement,
		exchange:        p.Exchange,
		accounts:        p.Accounts,
		bankOwner:       bankingAccount,
		distributeTable: NewNodeSizeTable(),
	}, nil
}
