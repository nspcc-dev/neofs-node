package intermediate

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	apiClient "github.com/nspcc-dev/neofs-sdk-go/client"
	reputationapi "github.com/nspcc-dev/neofs-sdk-go/reputation"
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
	eiContext, ok := ctx.(eigentrustcalc.Context)
	if !ok {
		// TODO: think if this can be done without such limitation
		panic(ErrIncorrectContextPanicMsg)
	}

	return &RemoteTrustWriter{
		eiCtx:  eiContext,
		client: twp.client,
		key:    twp.key,
	}, nil
}

type RemoteTrustWriter struct {
	eiCtx  eigentrustcalc.Context
	client apiClient.Client
	key    *ecdsa.PrivateKey
}

// Write sends trust value to remote node via ReputationService.AnnounceIntermediateResult RPC.
func (rtp *RemoteTrustWriter) Write(t reputation.Trust) error {
	apiTrustingPeer := reputationapi.NewPeerID()
	apiTrustingPeer.SetPublicKey(t.TrustingPeer())

	apiTrustedPeer := reputationapi.NewPeerID()
	apiTrustedPeer.SetPublicKey(t.Peer())

	apiTrust := reputationapi.NewTrust()
	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(apiTrustedPeer)

	apiPeerToPeerTrust := reputationapi.NewPeerToPeerTrust()
	apiPeerToPeerTrust.SetTrustingPeer(apiTrustingPeer)
	apiPeerToPeerTrust.SetTrust(apiTrust)

	var p internalclient.AnnounceIntermediatePrm

	p.SetContext(rtp.eiCtx)
	p.SetClient(rtp.client)
	p.SetPrivateKey(rtp.key)
	p.SetEpoch(rtp.eiCtx.Epoch())
	p.SetIteration(rtp.eiCtx.I())
	p.SetTrust(apiPeerToPeerTrust)

	_, err := internalclient.AnnounceIntermediate(p)

	return err
}

func (rtp *RemoteTrustWriter) Close() error {
	return nil
}
