package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// ChequePrm groups parameters of Cheque operation.
type ChequePrm struct {
	id     []byte
	user   util.Uint160
	amount int64
	lock   util.Uint160

	client.InvokePrmOptional
}

// SetID sets ID of the cheque.
func (c *ChequePrm) SetID(id []byte) {
	c.id = id
}

// SetUser sets user.
func (c *ChequePrm) SetUser(user util.Uint160) {
	c.user = user
}

// SetAmount sets amount.
func (c *ChequePrm) SetAmount(amount int64) {
	c.amount = amount
}

// SetLock sets lock.
func (c *ChequePrm) SetLock(lock util.Uint160) {
	c.lock = lock
}

// Cheque invokes `cheque` method of NeoFS contract.
func (x *Client) Cheque(p ChequePrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(chequeMethod)
	prm.SetArgs(p.id, p.user, p.amount, p.lock)
	prm.InvokePrmOptional = p.InvokePrmOptional

	return x.client.Invoke(prm)
}

// AlphabetUpdate update list of alphabet nodes.
func (x *Client) AlphabetUpdate(id []byte, pubs keys.PublicKeys) error {
	prm := client.InvokePrm{}
	prm.SetMethod(alphabetUpdateMethod)
	prm.SetArgs(id, pubs)

	return x.client.Invoke(prm)
}
