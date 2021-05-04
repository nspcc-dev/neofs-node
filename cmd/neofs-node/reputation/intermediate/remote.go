package intermediate

import (
	"crypto/ecdsa"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	reputationapi "github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
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

	buf []*apiClient.SendIntermediateTrustPrm
}

// Write check if passed context contains required
// data(returns ErrIncorrectContext if not) and
// caches passed trusts(as SendIntermediateTrustPrm structs).
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

	p := &apiClient.SendIntermediateTrustPrm{}
	p.SetEpoch(rtp.eiCtx.Epoch())
	p.SetIteration(rtp.eiCtx.I())
	p.SetTrust(apiPeerToPeerTrust)

	rtp.buf = append(rtp.buf, p)

	return nil
}

// Close sends all cached intermediate trusts.
// If error occurs, returns in immediately and stops iteration.
func (rtp *RemoteTrustWriter) Close() (err error) {
	for _, prm := range rtp.buf {
		_, err = rtp.client.SendIntermediateTrust(
			rtp.eiCtx,
			*prm,
			apiClient.WithKey(rtp.key),
		)
		if err != nil {
			return
		}
	}

	return
}
