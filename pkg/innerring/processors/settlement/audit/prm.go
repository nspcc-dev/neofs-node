package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"github.com/nspcc-dev/neofs-sdk-go/audit"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// CalculatorPrm groups the parameters of Calculator's constructor.
type CalculatorPrm struct {
	ResultStorage ResultStorage

	ContainerStorage common.ContainerStorage

	PlacementCalculator common.PlacementCalculator

	SGStorage SGStorage

	AccountStorage common.AccountStorage

	Exchanger common.Exchanger

	AuditFeeFetcher FeeFetcher
}

// ResultStorage is an interface of storage of the audit results.
type ResultStorage interface {
	// Must return all audit results by epoch number.
	AuditResultsForEpoch(epoch uint64) ([]*audit.Result, error)
}

// SGInfo groups the data about NeoFS storage group
// necessary for calculating audit fee.
type SGInfo interface {
	// Must return sum size of the all group members.
	Size() uint64
}

// SGStorage is an interface of storage of the storage groups.
type SGStorage interface {
	// Must return information about the storage group by address.
	SGInfo(*object.Address) (SGInfo, error)
}

// FeeFetcher wraps AuditFee method that returns audit fee price from
// the network configuration.
type FeeFetcher interface {
	AuditFee() (uint64, error)
}
