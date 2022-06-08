package netmap

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Source is an interface that wraps
// basic network map receiving method.
type Source interface {
	// GetNetMap reads the diff-th past network map from the storage.
	// Calling with zero diff returns the latest network map.
	// It returns the pointer to the requested network map and any error encountered.
	//
	// GetNetMap must return exactly one non-nil value.
	// GetNetMap must return ErrNotFound if the network map is not in the storage.
	//
	// Implementations must not retain the network map pointer and modify
	// the network map through it.
	GetNetMap(diff uint64) (*netmap.NetMap, error)

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
}

// GetLatestNetworkMap requests and returns the latest network map from the storage.
func GetLatestNetworkMap(src Source) (*netmap.NetMap, error) {
	return src.GetNetMap(0)
}

// GetPreviousNetworkMap requests and returns previous from the latest network map from the storage.
func GetPreviousNetworkMap(src Source) (*netmap.NetMap, error) {
	return src.GetNetMap(1)
}
