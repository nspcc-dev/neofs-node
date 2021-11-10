package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
)

// ChequePrm groups parameters of AlphabetUpdate operation.
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
func (x *ClientWrapper) Cheque(prm ChequePrm) error {
	args := neofscontract.ChequePrm{}

	args.SetID(prm.id)
	args.SetUser(prm.user)
	args.SetAmount(prm.amount)
	args.SetLock(prm.lock)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return x.client.Cheque(args)
}

// AlphabetUpdatePrm groups parameters of AlphabetUpdate operation.
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
func (x *ClientWrapper) AlphabetUpdate(prm AlphabetUpdatePrm) error {
	args := neofscontract.AlphabetUpdatePrm{}

	args.SetID(prm.id)
	args.SetPubs(prm.pubs)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return x.client.AlphabetUpdate(args)
}
