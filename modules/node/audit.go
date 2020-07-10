package node

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/services/public/object"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	cnrHandlerParams struct {
		*viper.Viper
		*zap.Logger
		Placer         implementations.ObjectPlacer
		PeerStore      peers.Store
		Peers          peers.Interface
		TimeoutsPrefix string
		Key            *ecdsa.PrivateKey

		TokenStore session.PrivateTokenStore
	}
)

func newObjectsContainerHandler(p cnrHandlerParams) (implementations.SelectiveContainerExecutor, error) {
	as, err := implementations.NewAddressStore(p.PeerStore, p.Logger)
	if err != nil {
		return nil, err
	}

	multiTransport, err := object.NewMultiTransport(object.MultiTransportParams{
		AddressStore:     as,
		EpochReceiver:    p.Placer,
		RemoteService:    object.NewRemoteService(p.Peers),
		Logger:           p.Logger,
		Key:              p.Key,
		PutTimeout:       p.Viper.GetDuration(p.TimeoutsPrefix + ".timeouts.put"),
		GetTimeout:       p.Viper.GetDuration(p.TimeoutsPrefix + ".timeouts.get"),
		HeadTimeout:      p.Viper.GetDuration(p.TimeoutsPrefix + ".timeouts.head"),
		SearchTimeout:    p.Viper.GetDuration(p.TimeoutsPrefix + ".timeouts.search"),
		RangeHashTimeout: p.Viper.GetDuration(p.TimeoutsPrefix + ".timeouts.range_hash"),
		DialTimeout:      p.Viper.GetDuration("object.dial_timeout"),

		PrivateTokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	exec, err := implementations.NewContainerTraverseExecutor(multiTransport)
	if err != nil {
		return nil, err
	}

	return implementations.NewObjectContainerHandler(implementations.ObjectContainerHandlerParams{
		NodeLister: p.Placer,
		Executor:   exec,
		Logger:     p.Logger,
	})
}
