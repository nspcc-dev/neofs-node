package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// ChequePrm groups the arguments of Cheque operation.
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
func (x *Client) Cheque(args ChequePrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.chequeMethod)
	prm.SetArgs(args.id, args.user.BytesBE(), args.amount, args.lock.BytesBE())
	prm.InvokePrmOptional = args.InvokePrmOptional

	return x.client.Invoke(prm)
}

// AlphabetUpdatePrm groups the arguments
// of alphabet nodes update invocation call.
type AlphabetUpdatePrm struct {
	id   []byte
	pubs keys.PublicKeys

	client.InvokePrmOptional
}

// SetID sets update ID.
func (a *AlphabetUpdatePrm) SetID(id []byte) {
	a.id = id
}

// SetPubs sets new alphabet public keys.
func (a *AlphabetUpdatePrm) SetPubs(pubs keys.PublicKeys) {
	a.pubs = pubs
}

// AlphabetUpdate update list of alphabet nodes.
func (x *Client) AlphabetUpdate(args AlphabetUpdatePrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.alphabetUpdateMethod)
	prm.SetArgs(args.id, args.pubs)
	prm.InvokePrmOptional = args.InvokePrmOptional

	return x.client.Invoke(prm)
}
