package netmap

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Source is an interface that wraps
// basic network map receiving method.
type Source interface {
	// GetNetMapByEpoch reads network map by the epoch number from the storage.
	// It returns the pointer to the requested network map and any error encountered.
	//
	// Must return exactly one non-nil value.
	//
	// Implementations must not retain the network map pointer and modify
	// the network map through it.
	GetNetMapByEpoch(epoch uint64) (*netmap.NetMap, error)

	// Epoch reads the current epoch from the storage.
	// It returns thw number of the current epoch and any error encountered.
	//
	// Must return exactly one non-default value.
	Epoch() (uint64, error)

	// NetMap returns current network map.
	NetMap() (*netmap.NetMap, error)
}
