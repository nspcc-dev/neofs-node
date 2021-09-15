package neofsid

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
)

// AccountKeys requests public keys of NeoFS account from NeoFS ID contract.
func (x *ClientWrapper) AccountKeys(id *owner.ID) (keys.PublicKeys, error) {
	var args neofsid.KeyListingArgs

	args.SetOwnerID(id.ToV2().GetValue())

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

// ManageKeys adds/removes list of public keys to/from NeoFS account.
func (x *ClientWrapper) ManageKeys(ownerID []byte, ks [][]byte, add bool) error {
	type args interface {
		SetOwnerID([]byte)
		SetKeys([][]byte)
	}

	var (
		a    args
		call func(args) error
	)

	if add {
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

	a.SetOwnerID(ownerID)
	a.SetKeys(ks)

	return call(a)
}
