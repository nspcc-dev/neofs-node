package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
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
	prm := client.InvokePrm{}

	prm.SetMethod(c.putMethod)
	prm.SetArgs(int64(args.epoch), args.peerID, args.value)

	err := c.client.Invoke(prm)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.putMethod, err)
	}
	return nil
}
