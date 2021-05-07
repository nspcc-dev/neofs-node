package local

import (
	"crypto/ecdsa"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	reputationapi "github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

// RemoteProviderPrm groups the required parameters of the RemoteProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type RemoteProviderPrm struct {
	Key *ecdsa.PrivateKey
}

// NewRemoteProvider creates a new instance of the RemoteProvider.
//
// Panics if at least one value of the parameters is invalid.
//
// The created RemoteProvider does not require additional
// initialization and is completely ready for work.
func NewRemoteProvider(prm RemoteProviderPrm) *RemoteProvider {
	switch {
	case prm.Key == nil:
		common.PanicOnPrmValue("NetMapSource", prm.Key)
	}

	return &RemoteProvider{
		key: prm.Key,
	}
}

// RemoteProvider is an implementation of the clientKeyRemoteProvider interface.
type RemoteProvider struct {
	key *ecdsa.PrivateKey
}

func (rp RemoteProvider) WithClient(c apiClient.Client) reputationcommon.WriterProvider {
	return &TrustWriterProvider{
		client: c,
		key:    rp.key,
	}
}

type TrustWriterProvider struct {
	client apiClient.Client
	key    *ecdsa.PrivateKey
}

func (twp *TrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &RemoteTrustWriter{
		ctx:    ctx,
		client: twp.client,
		key:    twp.key,
	}, nil
}

type RemoteTrustWriter struct {
	ctx    reputationcommon.Context
	client apiClient.Client
	key    *ecdsa.PrivateKey

	buf []*reputationapi.Trust
}

func (rtp *RemoteTrustWriter) Write(t reputation.Trust) error {
	apiTrust := reputationapi.NewTrust()

	apiPeer := reputationapi.NewPeerID()
	apiPeer.SetPublicKey(t.Peer())

	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(apiPeer)

	rtp.buf = append(rtp.buf, apiTrust)

	return nil
}

func (rtp *RemoteTrustWriter) Close() error {
	prm := apiClient.AnnounceLocalTrustPrm{}

	prm.SetEpoch(rtp.ctx.Epoch())
	prm.SetTrusts(rtp.buf)

	_, err := rtp.client.AnnounceLocalTrust(
		rtp.ctx,
		prm,
		apiClient.WithKey(rtp.key),
	)

	return err
}
