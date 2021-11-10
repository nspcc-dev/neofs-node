package neofscontract

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
)

// ManageKeysPrm groups parameters of ManageKeys operation.
type ManageKeysPrm struct {
	scriptHash []byte
	ks         [][]byte
	bind       bool

	client.InvokePrmOptional
}

// SetScriptHash sets script hash.
func (m *ManageKeysPrm) SetScriptHash(scriptHash []byte) {
	m.scriptHash = scriptHash
}

// SetKeys sets keys.
func (m *ManageKeysPrm) SetKeys(ks [][]byte) {
	m.ks = ks
}

// SetBind sets operation type: bind/unbind.
func (m *ManageKeysPrm) SetBind(bind bool) {
	m.bind = bind
}

// ManageKeys binds/unbinds list of public keys from NeoFS account by script hash.
func (x *ClientWrapper) ManageKeys(prm ManageKeysPrm) error {
	type args interface {
		SetScriptHash([]byte)
		SetKeys([][]byte)
		SetOptionalPrm(optional client.InvokePrmOptional)
	}

	var (
		a    args
		call func(args) error
	)

	if prm.bind {
		a = new(neofscontract.BindKeysArgs)
		call = func(a args) error {
			return x.client.BindKeys(*a.(*neofscontract.BindKeysArgs))
		}
	} else {
		a = new(neofscontract.UnbindKeysArgs)
		call = func(a args) error {
			return x.client.UnbindKeys(*a.(*neofscontract.UnbindKeysArgs))
		}
	}

	a.SetScriptHash(prm.scriptHash)
	a.SetKeys(prm.ks)
	a.SetOptionalPrm(prm.InvokePrmOptional)

	return call(a)
}
