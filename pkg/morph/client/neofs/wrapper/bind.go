package neofscontract

import (
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
)

// ManageKeys binds/unbinds list of public keys from NeoFS account by script hash.
func (x *ClientWrapper) ManageKeys(scriptHash []byte, ks [][]byte, bind bool) error {
	type args interface {
		SetScriptHash([]byte)
		SetKeys([][]byte)
	}

	var (
		a    args
		call func(args) error
	)

	if bind {
		a = new(neofscontract.BindKeysArgs)
		call = func(a args) error {
			return (*neofscontract.Client)(x).BindKeys(*a.(*neofscontract.BindKeysArgs))
		}
	} else {
		a = new(neofscontract.UnbindKeysArgs)
		call = func(a args) error {
			return (*neofscontract.Client)(x).UnbindKeys(*a.(*neofscontract.UnbindKeysArgs))
		}
	}

	a.SetScriptHash(scriptHash)
	a.SetKeys(ks)

	return call(a)
}
