package neofsid

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type CommonBindPrm struct {
	ownerID []byte // NeoFS account identifier

	keys [][]byte // list of serialized public keys

	client.InvokePrmOptional
}

func (x *CommonBindPrm) SetOptionalPrm(prm client.InvokePrmOptional) {
	x.InvokePrmOptional = prm
}

// SetOwnerID sets NeoFS account identifier.
func (x *CommonBindPrm) SetOwnerID(v []byte) {
	x.ownerID = v
}

// SetKeys sets a list of public keys in a binary format.
func (x *CommonBindPrm) SetKeys(v [][]byte) {
	x.keys = v
}

// AddKeys adds a list of public keys to/from NeoFS account.
func (x *Client) AddKeys(p CommonBindPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(addKeysMethod)
	prm.SetArgs(p.ownerID, p.keys)
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addKeysMethod, err)
	}

	return nil
}

// RemoveKeys removes a list of public keys to/from NeoFS account.
func (x *Client) RemoveKeys(args CommonBindPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(removeKeysMethod)
	prm.SetArgs(args.ownerID, args.keys)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", removeKeysMethod, err)
	}

	return nil
}
