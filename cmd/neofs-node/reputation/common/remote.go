package common

import (
	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	"github.com/pkg/errors"
)

type clientCache interface {
	Get(string) (apiClient.Client, error)
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
	localAddrSrc    network.LocalAddressSource
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
	LocalAddrSrc    network.LocalAddressSource
	DeadEndProvider reputationcommon.WriterProvider
	ClientCache     clientCache
	WriterProvider  clientKeyRemoteProvider
}

func NewRemoteTrustProvider(prm RemoteProviderPrm) reputationrouter.RemoteWriterProvider {
	switch {
	case prm.LocalAddrSrc == nil:
		PanicOnPrmValue("LocalAddrSrc", prm.LocalAddrSrc)
	case prm.DeadEndProvider == nil:
		PanicOnPrmValue("DeadEndProvider", prm.DeadEndProvider)
	case prm.ClientCache == nil:
		PanicOnPrmValue("ClientCache", prm.ClientCache)
	case prm.WriterProvider == nil:
		PanicOnPrmValue("WriterProvider", prm.WriterProvider)
	}

	return &remoteTrustProvider{
		localAddrSrc:    prm.LocalAddrSrc,
		deadEndProvider: prm.DeadEndProvider,
		clientCache:     prm.ClientCache,
		remoteProvider:  prm.WriterProvider,
	}
}

func (rtp *remoteTrustProvider) InitRemote(srv reputationcommon.ServerInfo) (reputationcommon.WriterProvider, error) {
	if srv == nil {
		return rtp.deadEndProvider, nil
	}

	addr := srv.Address()

	if rtp.localAddrSrc.LocalAddress().String() == srv.Address() {
		// if local => return no-op writer
		return trustcontroller.SimpleWriterProvider(new(NopReputationWriter)), nil
	}

	hostAddr, err := network.HostAddrFromMultiaddr(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert address to IP format")
	}

	c, err := rtp.clientCache.Get(hostAddr)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize API client")
	}

	return rtp.remoteProvider.WithClient(c), nil
}
