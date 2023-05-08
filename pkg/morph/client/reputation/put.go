package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

type (
	// PutPrm groups the arguments of "put reputation value" invocation call.
	PutPrm struct {
		epoch  uint64
		peerID reputation.PeerID
		value  reputation.GlobalTrust
	}
)

// SetEpoch sets epoch of reputation value.
func (p *PutPrm) SetEpoch(v uint64) {
	p.epoch = v
}

// SetPeerID sets peer id of reputation value.
func (p *PutPrm) SetPeerID(v reputation.PeerID) {
	p.peerID = v
}

// SetValue sets reputation value.
func (p *PutPrm) SetValue(v reputation.GlobalTrust) {
	p.value = v
}

// Put invokes direct call of "put reputation value" method of reputation contract.
func (c *Client) Put(p PutPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(putMethod)
	prm.SetArgs(p.epoch, p.peerID.PublicKey(), p.value.Marshal())

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putMethod, err)
	}
	return nil
}
