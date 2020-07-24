package node

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	object "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	cnrHandlerParams struct {
		*viper.Viper
		*zap.Logger
		Placer         *placement.PlacementWrapper
		PeerStore      peers.Store
		Peers          peers.Interface
		TimeoutsPrefix string
		Key            *ecdsa.PrivateKey

		TokenStore session.PrivateTokenStore
	}
)

func newObjectsContainerHandler(p cnrHandlerParams) (transport.SelectiveContainerExecutor, error) {
	as, err := storage.NewAddressStore(p.PeerStore, p.Logger)
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

	exec, err := transport.NewContainerTraverseExecutor(multiTransport)
	if err != nil {
		return nil, err
	}

	return transport.NewObjectContainerHandler(transport.ObjectContainerHandlerParams{
		NodeLister: p.Placer,
		Executor:   exec,
		Logger:     p.Logger,
	})
}
