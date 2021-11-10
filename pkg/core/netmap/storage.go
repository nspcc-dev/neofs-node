package netmap

import (
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Source is an interface that wraps
// basic network map receiving method.
type Source interface {
	// GetNetMap reads the diff-th past network map from the storage.
	// Calling with zero diff returns latest network map.
	// It returns the pointer to requested network map and any error encountered.
	//
	// GetNetMap must return exactly one non-nil value.
	// GetNetMap must return ErrNotFound if the network map is not in storage.
	//
	// Implementations must not retain the network map pointer and modify
	// the network map through it.
	GetNetMap(diff uint64) (*netmap.Netmap, error)

	// GetNetMapByEpoch reads network map by the epoch number from the storage.
	// It returns the pointer to requested network map and any error encountered.
	//
	// Must return exactly one non-nil value.
	//
	// Implementations must not retain the network map pointer and modify
	// the network map through it.
	GetNetMapByEpoch(epoch uint64) (*netmap.Netmap, error)

	// Epoch reads current epoch from the storage.
	// It returns number of the current epoch and any error encountered.
	//
	// Must return exactly one non-default value.
	Epoch() (uint64, error)
}

// GetLatestNetworkMap requests and returns latest network map from storage.
func GetLatestNetworkMap(src Source) (*netmap.Netmap, error) {
	return src.GetNetMap(0)
}

// GetPreviousNetworkMap requests and returns previous from latest network map from storage.
func GetPreviousNetworkMap(src Source) (*netmap.Netmap, error) {
	return src.GetNetMap(1)
}
