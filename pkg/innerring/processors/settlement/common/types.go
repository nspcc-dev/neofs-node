package common

import (
	"math/big"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// NodeInfo groups the data about the storage node
// necessary for calculating audit fees.
type NodeInfo interface {
	// Must return storage price of the node for one epoch in GASe-12.
	Price() *big.Int

	// Must return public key of the node.
	PublicKey() []byte
}

// ContainerInfo groups the data about NeoFS container
// necessary for calculating audit fee.
type ContainerInfo interface {
	// Must return identifier of the container owner.
	Owner() user.ID
}

// ContainerStorage is an interface of
// storage of the NeoFS containers.
type ContainerStorage interface {
	// Must return information about the container by ID.
	ContainerInfo(cid.ID) (ContainerInfo, error)
}

// PlacementCalculator is a component interface
// that builds placement vectors.
type PlacementCalculator interface {
	// Must return information about the nodes from container by its ID of the given epoch.
	ContainerNodes(uint64, cid.ID) ([]NodeInfo, error)
}

// AccountStorage is an network member accounts interface.
type AccountStorage interface {
	// Must resolve information about the storage node
	// to its ID in system.
	ResolveKey(NodeInfo) (*user.ID, error)
}

// Exchanger is an interface of monetary component.
type Exchanger interface {
	// Must transfer amount of GASe-12 from sender to recipient.
	//
	// Amount must be positive.
	Transfer(sender, recipient user.ID, amount *big.Int, details []byte)
}
