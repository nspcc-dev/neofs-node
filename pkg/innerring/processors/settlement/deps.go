package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/basic"
)

// AuditProcessor is an interface of data audit fee processor.
type AuditProcessor interface {
	// Must process data audit conducted in epoch.
	ProcessAuditSettlements(epoch uint64)
}

// BasicIncomeInitializer is an interface of basic income context creator.
type BasicIncomeInitializer interface {
	// Creates context that processes basic income for provided epoch.
	CreateContext(epoch uint64) (*basic.IncomeSettlementContext, error)
}
