package alphabet

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// NeoFS represents virtual connection to the NeoFS network.
type NeoFS interface {
	// StorageNodes lists accounts of all storage nodes presented in the network.
	// Returns an error in case of storage nodes' absence.
	StorageNodes() ([]util.Uint160, error)

	// StorageEmissionAmount returns amount of the sidechain GAS emitted to all
	// storage nodes once per GAS emission cycle. Returns models.ErrStorageEmissionDisabled
	// if storage emission is disabled in the network.
	StorageEmissionAmount() (uint64, error)
}

// helper type which is defined to separate the local node interface from NeoFS
// in the Processor type.
type node interface {
	// EmitSidechainGAS triggers production of the sidechain GAS and its
	// distribution among Inner Ring nodes and Proxy contract. Returns
	// models.ErrNonAlphabet if the node is not an Alphabet one.
	EmitSidechainGAS() error

	// TransferGAS sends request to transfer specified amount of GAS from the
	// node account to the given receiver. Final transfer is not guaranteed.
	//
	// Amount is always positive.
	TransferGAS(amount uint64, to util.Uint160) error
}

// LocalNode provides functionality of the local Inner Ring node which is expected
// by the Processor to work.
type LocalNode interface {
	node

	// NeoFS functionality is also provided by the node: it is highlighted to
	// differentiate between global system services and local node services.
	NeoFS
}

// Processor handles events spawned by the Alphabet contracts deployed
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
