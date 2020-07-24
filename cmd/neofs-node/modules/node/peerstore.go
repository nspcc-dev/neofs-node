package node

import (
	"crypto/ecdsa"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type peerstoreParams struct {
	dig.In

	Logger     *zap.Logger
	PrivateKey *ecdsa.PrivateKey
	Address    multiaddr.Multiaddr
	Store      peers.Storage `optional:"true"`
}

func newPeerstore(p peerstoreParams) (peers.Store, error) {
	return peers.NewStore(peers.StoreParams{
		Storage: p.Store,
		Logger:  p.Logger,
		Addr:    p.Address,
		Key:     p.PrivateKey,
	})
}
