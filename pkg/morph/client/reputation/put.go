package reputation

import (
	"github.com/pkg/errors"
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
	return errors.Wrapf(c.client.Invoke(
		c.putMethod,
		int64(args.epoch),
		args.peerID,
		args.value,
	), "could not invoke method (%s)", c.putMethod)
}

// PutViaNotary invokes notary call of "put reputation value" method of
// reputation contract.
func (c *Client) PutViaNotary(args PutArgs) error {
	return errors.Wrapf(c.client.NotaryInvoke(
		c.putMethod,
		int64(args.epoch),
		args.peerID,
		args.value,
	), "could not invoke method (%s)", c.putMethod)
}
