package neofsid

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// KeyListingArgs groups the arguments
// of key listing call.
type KeyListingArgs struct {
	ownerID []byte // account identifier
}

// KeyListingValues groups the stack parameters
// returned by key listing call.
type KeyListingValues struct {
	keys [][]byte // list of user public keys in binary format
}

// SetOwnerID sets the NeoFS account identifier
// in a binary format.
func (l *KeyListingArgs) SetOwnerID(v []byte) {
	l.ownerID = v
}

// Keys returns the list of account keys
// in a binary format.
func (l *KeyListingValues) Keys() [][]byte {
	return l.keys
}

// AccountKeys requests public keys of NeoFS account
// through method of NeoFS ID contract.
func (x *Client) AccountKeys(args KeyListingArgs) (*KeyListingValues, error) {
	invokeArgs := make([]interface{}, 0, 1)

	invokeArgs = append(invokeArgs, args.ownerID)

	items, err := x.client.TestInvoke(
		x.keyListingMethod,
		invokeArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", x.keyListingMethod, err)
	} else if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", x.keyListingMethod, ln)
	}

	items, err = client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("1st stack item must be an array (%s)", x.keyListingMethod)
	}

	keys := make([][]byte, 0, len(items))

	for i := range items {
		key, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("invalid stack item, expected byte array (%s)", x.keyListingMethod)
		}

		keys = append(keys, key)
	}

	return &KeyListingValues{
		keys: keys,
	}, nil
}
