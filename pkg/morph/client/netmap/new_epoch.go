package netmap

import "github.com/pkg/errors"

// NewEpochArgs groups the arguments
// of new epoch invocation call.
type NewEpochArgs struct {
	number int64 // new epoch number
}

// SetEpochNumber sets the new epoch number.
func (a *NewEpochArgs) SetEpochNumber(v int64) {
	a.number = v
}

// NewEpoch invokes the call of new epoch method
// of NeoFS Netmap contract.
func (c *Client) NewEpoch(args NewEpochArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.addPeerMethod,
		args.number,
	), "could not invoke method (%s)", c.newEpochMethod)
}
