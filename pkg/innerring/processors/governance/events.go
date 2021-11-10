package governance

import "github.com/nspcc-dev/neo-go/pkg/util"

// Sync is an event to start governance synchronization.
type Sync struct {
	// txHash is used in notary environmental
	// for calculating unique but same for
	// all notification receivers values.
	txHash util.Uint256
}

// TxHash returns hash of the TX that triggers
// synchronization process.
func (s Sync) TxHash() util.Uint256 {
	return s.txHash
}

// MorphEvent implements Event interface.
func (s Sync) MorphEvent() {}

// NewSyncEvent creates Sync event that was produced
// in transaction with txHash hash.
func NewSyncEvent(txHash util.Uint256) Sync {
	return Sync{txHash: txHash}
}
