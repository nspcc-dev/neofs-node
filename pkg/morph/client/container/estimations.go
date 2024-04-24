package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// StartEstimationPrm groups parameters of StartEstimation operation.
type StartEstimationPrm struct {
	commonEstimationPrm
}

// StopEstimationPrm groups parameters of StopEstimation operation.
type StopEstimationPrm struct {
	commonEstimationPrm
}

type commonEstimationPrm struct {
	epoch uint64

	client.InvokePrmOptional
}

// SetEpoch sets epoch.
func (p *commonEstimationPrm) SetEpoch(epoch uint64) {
	p.epoch = epoch
}

// StartEstimation votes to produce start estimation notification.
func (c *Client) StartEstimation(p StartEstimationPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(startEstimationMethod)
	prm.SetArgs(p.epoch)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", startEstimationMethod, err)
	}
	return nil
}

// StopEstimation votes to produce stop estimation notification.
func (c *Client) StopEstimation(p StopEstimationPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(stopEstimationMethod)
	prm.SetArgs(p.epoch)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", stopEstimationMethod, err)
	}
	return nil
}
