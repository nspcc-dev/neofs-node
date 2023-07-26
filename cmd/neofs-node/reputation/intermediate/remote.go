package intermediate

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/internal/client"
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	reputationapi "github.com/nspcc-dev/neofs-sdk-go/reputation"
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

func (rp RemoteProvider) WithClient(c coreclient.Client) reputationcommon.WriterProvider {
	return &TrustWriterProvider{
		client: c,
		key:    rp.key,
		log:    rp.log,
	}
}

type TrustWriterProvider struct {
	client coreclient.Client
	key    *ecdsa.PrivateKey
	log    *zap.Logger
}

func (twp *TrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	eiContext, ok := ctx.(eigentrustcalc.Context)
	if !ok {
		// TODO: #1164 think if this can be done without such limitation
		panic(ErrIncorrectContextPanicMsg)
	}

	return &RemoteTrustWriter{
		eiCtx:  eiContext,
		client: twp.client,
		key:    twp.key,
		log:    twp.log,
	}, nil
}

type RemoteTrustWriter struct {
	eiCtx  eigentrustcalc.Context
	client coreclient.Client
	key    *ecdsa.PrivateKey
	log    *zap.Logger
}

// Write sends a trust value to a remote node via ReputationService.AnnounceIntermediateResult RPC.
func (rtp *RemoteTrustWriter) Write(t reputation.Trust) error {
	epoch := rtp.eiCtx.Epoch()
	i := rtp.eiCtx.I()

	rtp.log.Debug("announcing trust",
		zap.Uint64("epoch", epoch),
		zap.Uint32("iteration", i),
		zap.Stringer("trusting_peer", t.TrustingPeer()),
		zap.Stringer("trusted_peer", t.Peer()),
	)

	var apiTrust reputationapi.Trust
	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(t.Peer())

	var apiPeerToPeerTrust reputationapi.PeerToPeerTrust
	apiPeerToPeerTrust.SetTrustingPeer(t.TrustingPeer())
	apiPeerToPeerTrust.SetTrust(apiTrust)

	var p internalclient.AnnounceIntermediatePrm

	p.SetContext(rtp.eiCtx)
	p.SetClient(rtp.client)
	p.SetEpoch(epoch)
	p.SetIteration(i)
	p.SetTrust(apiPeerToPeerTrust)

	_, err := internalclient.AnnounceIntermediate(p)

	return err
}

func (rtp *RemoteTrustWriter) Close() error {
	return nil
}
