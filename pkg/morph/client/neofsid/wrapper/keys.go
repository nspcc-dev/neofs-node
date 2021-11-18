package neofsid

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

// AccountKeysPrm groups parameters of AccountKeys operation.
type AccountKeysPrm struct {
	id *owner.ID
}

// SetID sets owner ID.
func (a *AccountKeysPrm) SetID(id *owner.ID) {
	a.id = id
}

// AccountKeys requests public keys of NeoFS account from NeoFS ID contract.
func (x *ClientWrapper) AccountKeys(prm AccountKeysPrm) (keys.PublicKeys, error) {
	var args neofsid.KeyListingArgs

	args.SetOwnerID(prm.id.ToV2().GetValue())

	res, err := x.client.AccountKeys(args)
	if err != nil {
		return nil, err
	}

	binKeys := res.Keys()
	ks := make(keys.PublicKeys, 0, len(binKeys))

	curve := elliptic.P256()

	for i := range binKeys {
		k, err := keys.NewPublicKeyFromBytes(binKeys[i], curve)
		if err != nil {
			return nil, fmt.Errorf("received invalid key: %w", err)
		}

		ks = append(ks, k)
	}

	return ks, nil
}

// ManageKeysPrm groups parameters of ManageKeys operation.
type ManageKeysPrm struct {
	ownerID []byte
	ks      [][]byte
	add     bool

	client.InvokePrmOptional
}

// SetOwnerID sets Owner ID.
func (m *ManageKeysPrm) SetOwnerID(ownerID []byte) {
	m.ownerID = ownerID
}

// SetKeys sets keys to add/remove.
func (m *ManageKeysPrm) SetKeys(ks [][]byte) {
	m.ks = ks
}

// SetAdd sets operation type.
func (m *ManageKeysPrm) SetAdd(add bool) {
	m.add = add
}

// ManageKeys adds/removes list of public keys to/from NeoFS account.
func (x *ClientWrapper) ManageKeys(prm ManageKeysPrm) error {
	type args interface {
		SetOwnerID([]byte)
		SetKeys([][]byte)
		SetOptionalPrm(optional client.InvokePrmOptional)
	}

	var (
		a    args
		call func(args) error
	)

	if prm.add {
		a = new(neofsid.AddKeysArgs)
		call = func(a args) error {
			return x.client.AddKeys(*a.(*neofsid.AddKeysArgs))
		}
	} else {
		a = new(neofsid.RemoveKeysArgs)
		call = func(a args) error {
			return x.client.RemoveKeys(*a.(*neofsid.RemoveKeysArgs))
		}
	}

	a.SetOwnerID(prm.ownerID)
	a.SetKeys(prm.ks)
	a.SetOptionalPrm(prm.InvokePrmOptional)

	return call(a)
}
