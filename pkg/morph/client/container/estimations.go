package container

import (
	"fmt"
)

type StartEstimation struct {
	epoch int64
}

func (e *StartEstimation) SetEpoch(v int64) {
	e.epoch = v
}

type StopEstimation struct {
	epoch int64
}

func (e *StopEstimation) SetEpoch(v int64) {
	e.epoch = v
}

func (c *Client) StartEstimation(args StartEstimation) error {
	if err := c.client.Invoke(c.startEstimation, args.epoch); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.startEstimation, err)
	}
	return nil
}

func (c *Client) StopEstimation(args StopEstimation) error {
	if err := c.client.Invoke(c.stopEstimation, args.epoch); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.stopEstimation, err)
	}
	return nil
}
