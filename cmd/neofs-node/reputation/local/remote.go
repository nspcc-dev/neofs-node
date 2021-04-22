package local

import (
	"crypto/ecdsa"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	reputationapi "github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	reputationutil "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	"github.com/pkg/errors"
)

type clientCache interface {
	Get(string) (apiClient.Client, error)
}

// remoteTrustProvider is implementation of reputation RemoteWriterProvider interface.
type remoteTrustProvider struct {
	localAddrSrc    network.LocalAddressSource
	deadEndProvider reputationcommon.WriterProvider
	key             *ecdsa.PrivateKey

	clientCache clientCache
}

// RemoteProviderPrm groups the required parameters of the remoteTrustProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type RemoteProviderPrm struct {
	LocalAddrSrc    network.LocalAddressSource
	DeadEndProvider reputationcommon.WriterProvider
	Key             *ecdsa.PrivateKey
	ClientCache     clientCache
}

func NewRemoteTrustProvider(prm RemoteProviderPrm) reputationrouter.RemoteWriterProvider {
	switch {
	case prm.LocalAddrSrc == nil:
		reputationutil.PanicOnPrmValue("LocalAddrSrc", prm.LocalAddrSrc)
	case prm.DeadEndProvider == nil:
		reputationutil.PanicOnPrmValue("DeadEndProvider", prm.DeadEndProvider)
	case prm.Key == nil:
		reputationutil.PanicOnPrmValue("Key", prm.Key)
	case prm.ClientCache == nil:
		reputationutil.PanicOnPrmValue("ClientCache", prm.ClientCache)
	}

	return &remoteTrustProvider{
		localAddrSrc:    prm.LocalAddrSrc,
		deadEndProvider: prm.DeadEndProvider,
		key:             prm.Key,
		clientCache:     prm.ClientCache,
	}
}

func (rtp *remoteTrustProvider) InitRemote(srv reputationrouter.ServerInfo) (reputationcommon.WriterProvider, error) {
	if srv == nil {
		return rtp.deadEndProvider, nil
	}

	addr := srv.Address()

	if rtp.localAddrSrc.LocalAddress().String() == srv.Address() {
		// if local => return no-op writer
		return trustcontroller.SimpleWriterProvider(new(reputationutil.NopReputationWriter)), nil
	}

	ipAddr, err := network.IPAddrFromMultiaddr(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert address to IP format")
	}

	c, err := rtp.clientCache.Get(ipAddr)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize API client")
	}

	return &RemoteTrustWriterProvider{
		client: c,
		key:    rtp.key,
	}, nil
}

type RemoteTrustWriterProvider struct {
	client apiClient.Client
	key    *ecdsa.PrivateKey
}

func (rtwp *RemoteTrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &RemoteTrustWriter{
		ctx:    ctx,
		client: rtwp.client,
		key:    rtwp.key,
	}, nil
}

type RemoteTrustWriter struct {
	ctx    reputationcommon.Context
	client apiClient.Client
	key    *ecdsa.PrivateKey

	buf []*reputationapi.Trust
}

func (rtp *RemoteTrustWriter) Write(_ reputationcommon.Context, t reputation.Trust) error {
	apiTrust := reputationapi.NewTrust()

	apiPeer := reputationapi.NewPeerID()
	apiPeer.SetPublicKey(t.Peer())

	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(apiPeer)

	rtp.buf = append(rtp.buf, apiTrust)

	return nil
}

func (rtp *RemoteTrustWriter) Close() error {
	prm := apiClient.SendLocalTrustPrm{}

	prm.SetEpoch(rtp.ctx.Epoch())
	prm.SetTrusts(rtp.buf)

	_, err := rtp.client.SendLocalTrust(
		rtp.ctx,
		prm,
		apiClient.WithKey(rtp.key),
	)

	return err
}
