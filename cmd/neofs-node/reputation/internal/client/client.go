package internal

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

type commonPrm struct {
	cli client.Client

	ctx context.Context

	epoch uint64

	opts []client.CallOption
}

// SetClient sets base client for NeoFS API communication.
//
// Required parameter.
func (x *commonPrm) SetClient(cli client.Client) {
	x.cli = cli
}

// SetContext sets context.Context for network communication.
//
// Required parameter.
func (x *commonPrm) SetContext(ctx context.Context) {
	x.ctx = ctx
}

// SetPrivateKey sets private key to sign the request(s).
//
// Required parameter.
func (x *commonPrm) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.opts = append(x.opts, client.WithKey(key))
}

// AnnounceLocalPrm groups parameters of AnnounceLocal operation.
type AnnounceLocalPrm struct {
	commonPrm

	cliPrm client.AnnounceLocalTrustPrm
}

// SetEpoch sets epoch in which the trust was assessed.
func (x *AnnounceLocalPrm) SetEpoch(epoch uint64) {
	x.cliPrm.SetEpoch(epoch)
}

// SetTrusts sets list of local trust values.
func (x *AnnounceLocalPrm) SetTrusts(ts []*reputation.Trust) {
	x.cliPrm.SetTrusts(ts)
}

// AnnounceLocalRes groups resulting values of AnnounceLocal operation.
type AnnounceLocalRes struct{}

// AnnounceLocal sends estimations of local trust to the remote node.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
func AnnounceLocal(prm AnnounceLocalPrm) (res AnnounceLocalRes, err error) {
	_, err = prm.cli.AnnounceLocalTrust(prm.ctx, prm.cliPrm, prm.opts...)

	return
}

// AnnounceIntermediatePrm groups parameters of AnnounceIntermediate operation.
type AnnounceIntermediatePrm struct {
	commonPrm

	cliPrm client.AnnounceIntermediateTrustPrm
}

// SetEpoch sets number of the epoch when the trust calculation's iteration was executed.
func (x *AnnounceIntermediatePrm) SetEpoch(epoch uint64) {
	x.cliPrm.SetEpoch(epoch)
}

// SetIteration sets number of the iteration of the trust calculation algorithm.
func (x *AnnounceIntermediatePrm) SetIteration(iter uint32) {
	x.cliPrm.SetIteration(iter)
}

// SetTrust sets current global trust value computed at the iteration.
func (x *AnnounceIntermediatePrm) SetTrust(t *reputation.PeerToPeerTrust) {
	x.cliPrm.SetTrust(t)
}

// AnnounceIntermediateRes groups resulting values of AnnounceIntermediate operation.
type AnnounceIntermediateRes struct{}

// AnnounceIntermediate sends global trust value calculated at the specified iteration
// and epoch to to the remote node.
//
// Client, context and key must be set.
//
// Returns any error prevented the operation from completing correctly in error return.
func AnnounceIntermediate(prm AnnounceIntermediatePrm) (res AnnounceIntermediateRes, err error) {
	_, err = prm.cli.AnnounceIntermediateTrust(prm.ctx, prm.cliPrm, prm.opts...)

	return
}
