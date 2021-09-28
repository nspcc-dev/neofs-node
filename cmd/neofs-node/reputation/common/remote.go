package common

import (
	"fmt"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
)

type clientCache interface {
	Get(client.NodeInfo) (apiClient.Client, error)
}

// clientKeyRemoteProvider must provide remote writer and take into account
// that requests must be sent via passed api client and must be signed with
// passed private key.
type clientKeyRemoteProvider interface {
	WithClient(apiClient.Client) reputationcommon.WriterProvider
}

// remoteTrustProvider is implementation of reputation RemoteWriterProvider interface.
// It caches clients, checks if it is the end of the route and checks either current
// node is remote target or not.
//
// remoteTrustProvider requires to be provided with clientKeyRemoteProvider.
type remoteTrustProvider struct {
	netmapKeys      netmap.AnnouncedKeys
	deadEndProvider reputationcommon.WriterProvider
	clientCache     clientCache
	remoteProvider  clientKeyRemoteProvider
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
	}

	return &remoteTrustProvider{
		netmapKeys:      prm.NetmapKeys,
		deadEndProvider: prm.DeadEndProvider,
		clientCache:     prm.ClientCache,
		remoteProvider:  prm.WriterProvider,
	}
}

func (rtp *remoteTrustProvider) InitRemote(srv reputationcommon.ServerInfo) (reputationcommon.WriterProvider, error) {
	if srv == nil {
		return rtp.deadEndProvider, nil
	}

	if rtp.netmapKeys.IsLocalKey(srv.PublicKey()) {
		// if local => return no-op writer
		return trustcontroller.SimpleWriterProvider(new(NopReputationWriter)), nil
	}

	var netAddr network.AddressGroup

	err := netAddr.FromIterator(srv)
	if err != nil {
		return nil, fmt.Errorf("could not convert address to IP format: %w", err)
	}

	var info client.NodeInfo

	info.SetAddressGroup(netAddr)

	c, err := rtp.clientCache.Get(info)
	if err != nil {
		return nil, fmt.Errorf("could not initialize API client: %w", err)
	}

	return rtp.remoteProvider.WithClient(c), nil
}
