package neofscontract

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
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

	client.InvokePrmOptional
}

// SetOptionalPrm sets optional client parameters.
func (x *commonBindArgs) SetOptionalPrm(op client.InvokePrmOptional) {
	x.InvokePrmOptional = op
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
	prm := client.InvokePrm{}

	prm.SetMethod(bindKeysMethod)
	prm.SetArgs(args.scriptHash, args.keys)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", bindKeysMethod, err)
	}

	return nil
}

// UnbindKeys invokes the call of key unbinding method
// of NeoFS contract.
func (x *Client) UnbindKeys(args UnbindKeysArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(unbindKeysMethod)
	prm.SetArgs(args.scriptHash, args.keys)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", unbindKeysMethod, err)
	}

	return nil
}
