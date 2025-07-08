package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/basic"
)

// BasicIncomeInitializer is an interface of basic income context creator.
type BasicIncomeInitializer interface {
	// CreateContext creates context that processes basic income for provided epoch.
	CreateContext(epoch uint64) (*basic.IncomeSettlementContext, error)
}
