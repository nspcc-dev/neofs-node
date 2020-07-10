package implementations

import (
	"crypto/ecdsa"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// AddressStoreComponent is an interface of encapsulated AddressStore and NodePublicKeyReceiver pair.
	AddressStoreComponent interface {
		AddressStore
		NodePublicKeyReceiver
	}

	// AddressStore is an interface of the container of local Multiaddr.
	AddressStore interface {
		SelfAddr() (multiaddr.Multiaddr, error)
	}

	// NodePublicKeyReceiver is an interface of Multiaddr to PublicKey converter.
	NodePublicKeyReceiver interface {
		PublicKey(multiaddr.Multiaddr) *ecdsa.PublicKey
	}

	addressStore struct {
		ps peers.Store

		log *zap.Logger
	}
)

const (
	addressStoreInstanceFailMsg = "could not create address store"
	errEmptyPeerStore           = internal.Error("empty peer store")

	errEmptyAddressStore = internal.Error("empty address store")
)

func (s addressStore) SelfAddr() (multiaddr.Multiaddr, error) { return s.ps.GetAddr(s.ps.SelfID()) }

func (s addressStore) PublicKey(mAddr multiaddr.Multiaddr) (res *ecdsa.PublicKey) {
	if peerID, err := s.ps.AddressID(mAddr); err != nil {
		s.log.Error("could not peer ID",
			zap.Stringer("node", mAddr),
			zap.Error(err),
		)
	} else if res, err = s.ps.GetPublicKey(peerID); err != nil {
		s.log.Error("could not receive public key",
			zap.Stringer("peer", peerID),
			zap.Error(err),
		)
	}

	return res
}

// NewAddressStore wraps peer store and returns AddressStoreComponent.
func NewAddressStore(ps peers.Store, log *zap.Logger) (AddressStoreComponent, error) {
	if ps == nil {
		return nil, errors.Wrap(errEmptyPeerStore, addressStoreInstanceFailMsg)
	} else if log == nil {
		return nil, errors.Wrap(errEmptyLogger, addressStoreInstanceFailMsg)
	}

	return &addressStore{
		ps:  ps,
		log: log,
	}, nil
}
