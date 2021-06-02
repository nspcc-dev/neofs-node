package reputation

import (
	"fmt"
)

// PutArgs groups the arguments of "put reputation value" invocation call.
type PutArgs struct {
	epoch  uint64
	peerID []byte
	value  []byte
}

// SetEpoch sets epoch of reputation value.
func (p *PutArgs) SetEpoch(v uint64) {
	p.epoch = v
}

// SetPeerID sets peer id of reputation value.
func (p *PutArgs) SetPeerID(v []byte) {
	p.peerID = v
}

// SetValue sets marshaled reputation value.
func (p *PutArgs) SetValue(v []byte) {
	p.value = v
}

// Put invokes direct call of "put reputation value" method of reputation contract.
func (c *Client) Put(args PutArgs) error {
	err := c.client.Invoke(
		c.putMethod,
		int64(args.epoch),
		args.peerID,
		args.value,
	)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.putMethod, err)
	}
	return nil
}
