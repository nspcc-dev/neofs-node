package bootstrap

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
)

// ContractClient represents the Netmap
// contract client.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper.Wrapper.
type ContractClient = *wrapper.Wrapper

// NodeInfo represents the
// information about storage node.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/netmap.Info.
type NodeInfo = netmap.Info

// Registerer represents the tool that
// registers storage node in NeoFS system.
//
// Working Registerer must be created via constructor New.
// Using the Registerer that has been created with new(Registerer)
// expression (or just declaring a Registerer variable) is unsafe
// and can lead to panic.
type Registerer struct {
	client ContractClient

	info NodeInfo
}

// New creates, initializes and returns the Registerer instance.
//
// If passed contract client is nil, wrapper.ErrNilWrapper is returned.
func New(client ContractClient, info NodeInfo) (*Registerer, error) {
	if client == nil {
		return nil, wrapper.ErrNilWrapper
	}

	return &Registerer{
		client: client,
		info:   info,
	}, nil
}

// Bootstrap registers storage node in NeoFS system
// through Netmap contract client.
//
// If contract client returns error, panic arises without retry.
func (r *Registerer) Bootstrap(context.Context) {
	// register peer in NeoFS network
	if err := r.client.AddPeer(r.info); err != nil {
		panic(err)
	}
}
