package internal

import (
	"context"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

type commonPrm struct {
	cli coreclient.Client

	ctx context.Context
}

// SetClient sets the base client for NeoFS API communication.
//
// Required parameter.
func (x *commonPrm) SetClient(cli coreclient.Client) {
	x.cli = cli
}

// SetContext sets context.Context for network communication.
//
// Required parameter.
func (x *commonPrm) SetContext(ctx context.Context) {
	x.ctx = ctx
}

// AnnounceLocalPrm groups parameters of AnnounceLocal operation.
type AnnounceLocalPrm struct {
	commonPrm

	epoch  uint64
	trusts []reputation.Trust
	cliPrm client.PrmAnnounceLocalTrust
}

// SetEpoch sets the epoch in which the trust was assessed.
func (x *AnnounceLocalPrm) SetEpoch(epoch uint64) {
	x.epoch = epoch
}

// SetTrusts sets a list of local trust values.
func (x *AnnounceLocalPrm) SetTrusts(ts []reputation.Trust) {
	x.trusts = ts
}

// AnnounceLocalRes groups the resulting values of AnnounceLocal operation.
type AnnounceLocalRes struct{}

// AnnounceLocal sends estimations of local trust to the remote node.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
func AnnounceLocal(prm AnnounceLocalPrm) (res AnnounceLocalRes, err error) {
	err = prm.cli.AnnounceLocalTrust(prm.ctx, prm.epoch, prm.trusts, prm.cliPrm)
	return
}

// AnnounceIntermediatePrm groups parameters of AnnounceIntermediate operation.
type AnnounceIntermediatePrm struct {
	commonPrm

	epoch    uint64
	p2pTrust reputation.PeerToPeerTrust
	cliPrm   client.PrmAnnounceIntermediateTrust
}

// SetEpoch sets the number of the epoch when the trust calculation's iteration was executed.
func (x *AnnounceIntermediatePrm) SetEpoch(epoch uint64) {
	x.epoch = epoch
}

// SetIteration sets the number of the iteration of the trust calculation algorithm.
func (x *AnnounceIntermediatePrm) SetIteration(iter uint32) {
	x.cliPrm.SetIteration(iter)
}

// SetTrust sets the current global trust value computed at the iteration.
func (x *AnnounceIntermediatePrm) SetTrust(t reputation.PeerToPeerTrust) {
	x.p2pTrust = t
}

// AnnounceIntermediateRes groups the resulting values of AnnounceIntermediate operation.
type AnnounceIntermediateRes struct{}

// AnnounceIntermediate sends the global trust value calculated at the specified iteration
// and epoch to the remote node.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
func AnnounceIntermediate(prm AnnounceIntermediatePrm) (res AnnounceIntermediateRes, err error) {
	err = prm.cli.AnnounceIntermediateTrust(prm.ctx, prm.epoch, prm.p2pTrust, prm.cliPrm)
	return
}
