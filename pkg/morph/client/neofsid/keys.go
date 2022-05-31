package neofsid

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AccountKeysPrm groups parameters of AccountKeys operation.
type AccountKeysPrm struct {
	id user.ID
}

// SetID sets owner ID.
func (a *AccountKeysPrm) SetID(id user.ID) {
	a.id = id
}

// AccountKeys requests public keys of NeoFS account from NeoFS ID contract.
func (x *Client) AccountKeys(p AccountKeysPrm) (keys.PublicKeys, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(keyListingMethod)
	prm.SetArgs(p.id.WalletBytes())

	items, err := x.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", keyListingMethod, err)
	} else if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", keyListingMethod, ln)
	}

	items, err = client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("1st stack item must be an array (%s)", keyListingMethod)
	}

	pubs := make(keys.PublicKeys, len(items))
	for i := range items {
		rawPub, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("invalid stack item, expected byte array (%s)", keyListingMethod)
		}

		pubs[i], err = keys.NewPublicKeyFromBytes(rawPub, elliptic.P256())
		if err != nil {
			return nil, fmt.Errorf("received invalid key (%s): %w", keyListingMethod, err)
		}
	}

	return pubs, nil
}
