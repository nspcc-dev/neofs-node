package neofscontract

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

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

// SetKeys sets a list of public keys in a binary format.
func (x *commonBindArgs) SetKeys(v [][]byte) {
	x.keys = v
}

// BindKeysPrm groups parameters of BindKeys operation.
type BindKeysPrm struct {
	commonBindArgs
}

// BindKeys binds list of public keys from NeoFS account by script hash.
func (x *Client) BindKeys(p BindKeysPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(bindKeysMethod)
	prm.SetArgs(p.scriptHash, p.keys)
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := x.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", bindKeysMethod, err)
	}

	return nil
}

// UnbindKeysPrm groups parameters of UnbindKeys operation.
type UnbindKeysPrm struct {
	commonBindArgs
}

// UnbindKeys invokes the call of key unbinding method
// of NeoFS contract.
func (x *Client) UnbindKeys(args UnbindKeysPrm) error {
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
