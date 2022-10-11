package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// NeoFS represents virtual connection to the NeoFS network.
type NeoFS interface {
	// Authorize performs user authorization by checking the signature of the
	// given data with optional neofsecdsa.PublicKeyRFC6979. Nil return means
	// successful authorization.
	Authorize(usr user.ID, data, signature []byte, key *neofsecdsa.PublicKeyRFC6979) error

	// CheckUserAllowanceToSubnet checks if given user is allowed to bind his containers
	// to the specified subnet and returns:
	//  no error if he is
	//  models.ErrSubnetAccess if he is not
	//  any other error which prevented the access to be checked
	CheckUserAllowanceToSubnet(subnetid.ID, user.ID) error

	// ContainerCreator returns identifier of the user which created container
	// referenced by the given cid.ID.
	ContainerCreator(cid.ID) (user.ID, error)

	// IsContainerACLExtendable checks if ACL of the container referenced by
	// the given cid.ID is extendable. Returns any error encountered which
	// prevented the check to be done.
	IsContainerACLExtendable(cid.ID) (bool, error)

	// CurrentEpoch returns the number of the current epoch. Returns any error
	// encountered which did not allow to determine the epoch.
	CurrentEpoch() (uint64, error)

	// IsHomomorphicHashingDisabled checks if homomorphic hashing of the object data
	// is disabled in the network. Returns any error encountered which did not allow
	// to determine the property.
	IsHomomorphicHashingDisabled() (bool, error)
}

// helper type which is defined to separate the local node interface from NeoFS
// in the Processor type.
type node interface {
	common.NodeStatus
}

// LocalNode provides functionality of the local Inner Ring node which is expected
// by the Processor to work.
type LocalNode interface {
	node

	// NeoFS functionality is also provided by the node: it is highlighted to
	// differentiate between global system services and local node services.
	NeoFS
}

// Processor handles events spawned by the Container contract deployed
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
