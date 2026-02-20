package common

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	"go.uber.org/zap"
)

type clientCache interface {
	Get(context.Context, client.NodeInfo) (client.MultiAddressClient, error)
}

// clientKeyRemoteProvider must provide a remote writer and take into account
// that requests must be sent via the passed api client and must be signed with
// the passed private key.
type clientKeyRemoteProvider interface {
	WithClient(client.Client) reputationcommon.WriterProvider
}

// remoteTrustProvider is an implementation of reputation RemoteWriterProvider interface.
// It caches clients, checks if it is the end of the route and checks either the current
// node is a remote target or not.
//
// remoteTrustProvider requires to be provided with clientKeyRemoteProvider.
type remoteTrustProvider struct {
	netmapKeys      netmap.AnnouncedKeys
	deadEndProvider reputationcommon.WriterProvider
	clientCache     clientCache
	remoteProvider  clientKeyRemoteProvider
	log             *zap.Logger
}

// RemoteProviderPrm groups the required parameters of the remoteTrustProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type RemoteProviderPrm struct {
	NetmapKeys      netmap.AnnouncedKeys
	DeadEndProvider reputationcommon.WriterProvider
	ClientCache     clientCache
	WriterProvider  clientKeyRemoteProvider
	Log             *zap.Logger
}

func NewRemoteTrustProvider(prm RemoteProviderPrm) reputationrouter.RemoteWriterProvider {
	switch {
	case prm.NetmapKeys == nil:
		PanicOnPrmValue("NetmapKeys", prm.NetmapKeys)
	case prm.DeadEndProvider == nil:
		PanicOnPrmValue("DeadEndProvider", prm.DeadEndProvider)
	case prm.ClientCache == nil:
		PanicOnPrmValue("ClientCache", prm.ClientCache)
	case prm.WriterProvider == nil:
		PanicOnPrmValue("WriterProvider", prm.WriterProvider)
	case prm.Log == nil:
		PanicOnPrmValue("Logger", prm.Log)
	}

	return &remoteTrustProvider{
		netmapKeys:      prm.NetmapKeys,
		deadEndProvider: prm.DeadEndProvider,
		clientCache:     prm.ClientCache,
		remoteProvider:  prm.WriterProvider,
		log:             prm.Log,
	}
}

func (rtp *remoteTrustProvider) InitRemote(ctx context.Context, srv reputationcommon.ServerInfo) (reputationcommon.WriterProvider, error) {
	rtp.log.Debug("initializing remote writer provider")

	if srv == nil {
		rtp.log.Debug("route has reached dead-end provider")
		return rtp.deadEndProvider, nil
	}

	if rtp.netmapKeys.IsLocalKey(srv.PublicKey()) {
		// if local => return no-op writer
		rtp.log.Debug("initializing no-op writer provider")
		return trustcontroller.SimpleWriterProvider(new(NopReputationWriter)), nil
	}

	var info client.NodeInfo

	err := client.NodeInfoFromRawNetmapElement(&info, srv)
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	c, err := rtp.clientCache.Get(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("could not initialize API client: %w", err)
	}

	return rtp.remoteProvider.WithClient(c), nil
}
