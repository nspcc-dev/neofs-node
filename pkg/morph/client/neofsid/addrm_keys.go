package neofsid

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// AddKeysArgs groups the arguments
// of key binding call.
type AddKeysArgs struct {
	commonBindArgs
}

// RemoveKeysArgs groups the arguments
// of key unbinding call.
type RemoveKeysArgs struct {
	commonBindArgs
}

type commonBindArgs struct {
	ownerID []byte // NeoFS account identifier

	keys [][]byte // list of serialized public keys
}

// SetOwnerID sets NeoFS account identifier.
func (x *commonBindArgs) SetOwnerID(v []byte) {
	x.ownerID = v
}

// SetKeys sets list of public keys in a binary format.
func (x *commonBindArgs) SetKeys(v [][]byte) {
	x.keys = v
}

// AddKeys invokes the call of key adding method
// of NeoFS contract.
func (x *Client) AddKeys(args AddKeysArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.addKeysMethod)
	prm.SetArgs(args.ownerID, args.keys)

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.addKeysMethod, err)
	}

	return nil
}

// RemoveKeys invokes the call of key removing method
// of NeoFS contract.
func (x *Client) RemoveKeys(args RemoveKeysArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.removeKeysMethod)
	prm.SetArgs(args.ownerID, args.keys)

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.removeKeysMethod, err)
	}

	return nil
}
