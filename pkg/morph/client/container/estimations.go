package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// StartEstimation groups parameters of StartEstimation operation.
type StartEstimation struct {
	epoch int64

	client.InvokePrmOptional
}

func (e *StartEstimation) SetEpoch(v int64) {
	e.epoch = v
}

type StopEstimation struct {
	epoch int64

	client.InvokePrmOptional
}

func (e *StopEstimation) SetEpoch(v int64) {
	e.epoch = v
}

func (c *Client) StartEstimation(args StartEstimation) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.startEstimation)
	prm.SetArgs(args.epoch)
	prm.InvokePrmOptional = args.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.startEstimation, err)
	}
	return nil
}

func (c *Client) StopEstimation(args StopEstimation) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.stopEstimation)
	prm.SetArgs(args.epoch)
	prm.InvokePrmOptional = args.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.stopEstimation, err)
	}
	return nil
}
