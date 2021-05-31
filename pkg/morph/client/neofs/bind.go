package neofscontract

import (
	"fmt"
)

// BindKeysArgs groups the arguments
// of key binding call.
type BindKeysArgs struct {
	commonBindArgs
}

// UnbindKeysArgs groups the arguments
// of key unbinding call.
type UnbindKeysArgs struct {
	commonBindArgs
}

type commonBindArgs struct {
	scriptHash []byte // script hash of account identifier

	keys [][]byte // list of serialized public keys
}

// SetScriptHash sets script hash of the NeoFS account identifier.
func (x *commonBindArgs) SetScriptHash(v []byte) {
	x.scriptHash = v
}

// SetKeys sets list of public keys in a binary format.
func (x *commonBindArgs) SetKeys(v [][]byte) {
	x.keys = v
}

// BindKeys invokes the call of key binding method
// of NeoFS contract.
func (x *Client) BindKeys(args BindKeysArgs) error {
	err := x.client.Invoke(
		x.bindKeysMethod,
		args.scriptHash,
		args.keys,
	)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.bindKeysMethod, err)
	}

	return nil
}

// UnbindKeys invokes the call of key unbinding method
// of NeoFS contract.
func (x *Client) UnbindKeys(args UnbindKeysArgs) error {
	err := x.client.Invoke(
		x.unbindKeysMethod,
		args.scriptHash,
		args.keys,
	)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", x.unbindKeysMethod, err)
	}

	return nil
}
