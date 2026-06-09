package processors

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// MetadataChain describes metadata chain.
type MetadataChain interface {
	// UpdateContainerPlacement must update container placement list in
	// metadata chain. Must block until updated is finished.
	// Nonce will always be unique for the same other arguments.
	UpdateContainerPlacement(cid cid.ID, vectors [][]netmap.NodeInfo, policy netmap.PlacementPolicy, nonce uint32) error

	// RegisterMetadataContainer must register container as a metadata-enabled one.
	// Must block until updated is finished.
	// Nonce will always be unique for any call.
	RegisterMetadataContainer(cID cid.ID, nonce uint32) error
}
