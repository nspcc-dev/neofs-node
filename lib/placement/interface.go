package placement

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type (
	// Component is interface of placement service
	Component interface {
		// TODO leave for feature request

		NetworkState() *bootstrap.SpreadMap
		Neighbours(seed, epoch uint64, full bool) []peers.ID
		Update(epoch uint64, nm *netmap.NetMap) error
		Query(ctx context.Context, opts ...QueryOption) (Graph, error)
	}

	// QueryOptions for query request
	QueryOptions struct {
		CID      refs.CID
		Previous int
		Excludes []multiaddr.Multiaddr
	}

	// QueryOption settings closure
	QueryOption func(*QueryOptions)

	// FilterRule bucket callback handler
	FilterRule func(netmap.SFGroup, *netmap.Bucket) *netmap.Bucket

	// Graph is result of request to Placement-component
	Graph interface {
		Filter(rule FilterRule) Graph
		Exclude(list []multiaddr.Multiaddr) Graph
		NodeList() ([]multiaddr.Multiaddr, error)
		NodeInfo() ([]bootstrap.NodeInfo, error)
	}

	// Key to fetch node-list
	Key []byte

	// Params to create Placement component
	Params struct {
		Log                *zap.Logger
		Netmap             *netmap.NetMap
		Peerstore          peers.Store
		Fetcher            container.Storage
		ChronologyDuration uint64 // storing number of past epochs states
	}

	networkState struct {
		nm    *netmap.NetMap
		epoch uint64
	}

	// placement is implementation of placement.Component
	placement struct {
		log *zap.Logger
		cnr container.Storage

		chronologyDur uint64
		nmStore       *netMapStore

		ps peers.Store

		healthy *atomic.Bool
	}

	// graph is implementation of placement.Graph
	graph struct {
		roots []*netmap.Bucket
		items []bootstrap.NodeInfo
		place *netmap.PlacementRule
	}
)

// Copy network state.
func (ns networkState) Copy() *networkState {
	return &networkState{
		nm:    ns.nm.Copy(),
		epoch: ns.epoch,
	}
}

// ExcludeNodes to ignore some nodes.
func ExcludeNodes(list []multiaddr.Multiaddr) QueryOption {
	return func(opt *QueryOptions) {
		opt.Excludes = list
	}
}

// ContainerID set by Key.
func ContainerID(cid refs.CID) QueryOption {
	return func(opt *QueryOptions) {
		opt.CID = cid
	}
}

// UsePreviousNetmap for query.
func UsePreviousNetmap(diff int) QueryOption {
	return func(opt *QueryOptions) {
		opt.Previous = diff
	}
}
