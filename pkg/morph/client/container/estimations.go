package container

import (
	"github.com/pkg/errors"
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
	return errors.Wrapf(c.client.Invoke(
		c.startEstimation,
		args.epoch,
	), "could not invoke method (%s)", c.startEstimation)
}

func (c *Client) StopEstimation(args StopEstimation) error {
	return errors.Wrapf(c.client.Invoke(
		c.stopEstimation,
		args.epoch,
	), "could not invoke method (%s)", c.stopEstimation)
}
