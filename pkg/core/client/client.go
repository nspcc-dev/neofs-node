package client

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	rawclient "github.com/nspcc-dev/neofs-api-go/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	client.Client

	// RawForAddress must return rawclient.Client
	// for the passed network.Address.
	RawForAddress(network.Address) *rawclient.Client
}
