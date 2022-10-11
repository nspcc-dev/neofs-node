package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// NeoFS represents virtual connection to the NeoFS network.
type NeoFS interface {
	// BalanceSystemPrecision returns precision of the system balances.
	BalanceSystemPrecision() (uint64, error)
}

// helper type which is defined to separate the local node interface from NeoFS
// in the Processor type.
type node interface {
	common.NodeStatus

	// ApproveWithdrawal sends local node's approval to transfer specified amount
	// of GAS back from NeoFS system to the user account in the Neo Main Net.
	// Assets are expected to be preliminarily locked in the system by the
	// given withdrawal transaction.
	ApproveWithdrawal(userAcc util.Uint160, amount uint64, withdrawTx util.Uint256) error
}

// LocalNode provides functionality of the local Inner Ring node which is expected
// by the Processor to work.
type LocalNode interface {
	node

	// NeoFS functionality is also provided by the node: it is highlighted to
	// differentiate between global system services and local node services.
	NeoFS
}

// Processor handles events spawned by the Balance contract deployed
// in the NeoFS sidechain.
type Processor struct {
	log *logger.Logger

	node node

	neoFS NeoFS
}

// New creates and initializes new Processor instance using the provided parameters.
// All parameters are required.
func New(log *logger.Logger, node LocalNode) *Processor {
	if log == nil {
		panic("missing logger")
	} else if node == nil {
		panic("missing node interface")
	}

	return &Processor{
		log:   log,
		node:  node,
		neoFS: node,
	}
}
