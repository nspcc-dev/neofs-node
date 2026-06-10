package local

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/internal/client"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	localreputation "github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
	"go.uber.org/zap"
)

// RemoteProviderPrm groups the required parameters of the RemoteProvider's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type RemoteProviderPrm struct {
	Key *ecdsa.PrivateKey
	Log *zap.Logger
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
	case prm.Log == nil:
		common.PanicOnPrmValue("Logger", prm.Log)
	}

	return &RemoteProvider{
		key: prm.Key,
		log: prm.Log,
	}
}

// RemoteProvider is an implementation of the clientKeyRemoteProvider interface.
type RemoteProvider struct {
	key *ecdsa.PrivateKey
	log *zap.Logger
}

func (rp RemoteProvider) WithClient(c clientcore.Client) reputationcommon.WriterProvider {
	return &TrustWriterProvider{
		client: c,
		key:    rp.key,
		log:    rp.log,
	}
}

type TrustWriterProvider struct {
	client clientcore.Client
	key    *ecdsa.PrivateKey
	log    *zap.Logger
}

func (twp *TrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &RemoteTrustWriter{
		ctx:    ctx,
		client: twp.client,
		key:    twp.key,
		log:    twp.log,
	}, nil
}

type RemoteTrustWriter struct {
	ctx    reputationcommon.Context
	client clientcore.Client
	key    *ecdsa.PrivateKey
	log    *zap.Logger

	buf []reputation.Trust
}

func (rtp *RemoteTrustWriter) Write(t localreputation.Trust) error {
	var apiTrust reputation.Trust

	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(t.Peer())

	rtp.buf = append(rtp.buf, apiTrust)

	return nil
}

func (rtp *RemoteTrustWriter) Close() error {
	epoch := rtp.ctx.Epoch()

	rtp.log.Debug("announcing trusts",
		zap.Uint64("epoch", epoch),
	)

	var prm internalclient.AnnounceLocalPrm

	prm.SetContext(rtp.ctx)
	prm.SetClient(rtp.client)
	prm.SetEpoch(epoch)
	prm.SetTrusts(rtp.buf)

	_, err := internalclient.AnnounceLocal(prm)

	return err
}
