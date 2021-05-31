package neofsid

import (
	"fmt"
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
	err := x.client.Invoke(
		x.addKeysMethod,
		args.ownerID,
		args.keys,
	)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.addKeysMethod, err)
	}

	return nil
}

// RemoveKeys invokes the call of key removing method
// of NeoFS contract.
func (x *Client) RemoveKeys(args RemoveKeysArgs) error {
	err := x.client.Invoke(
		x.removeKeysMethod,
		args.ownerID,
		args.keys,
	)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.removeKeysMethod, err)
	}

	return nil
}
