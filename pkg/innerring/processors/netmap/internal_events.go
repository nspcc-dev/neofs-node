package netmap

import "github.com/nspcc-dev/neo-go/pkg/util"

// netmapCleanupTick is a event to remove offline nodes.
type netmapCleanupTick struct {
	epoch uint64

	// txHash is used in notary environment
	// for calculating unique but same for
	// all notification receivers values.
	txHash util.Uint256
}

// TxHash returns the hash of the TX that triggers
// synchronization process.
func (s netmapCleanupTick) TxHash() util.Uint256 {
	return s.txHash
}

// MorphEvent implements the Event interface.
func (netmapCleanupTick) MorphEvent() {}
