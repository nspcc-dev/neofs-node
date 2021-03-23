package invoke

import (
	"bytes"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// InnerRingIndex returns index of the `key` in the inner ring list from sidechain
// along with total size of inner ring list. If key is not in the inner ring list,
// then returns `-1` as index.
func InnerRingIndex(cli *client.Client, key *ecdsa.PublicKey) (int32, int32, error) {
	if cli == nil {
		return 0, 0, client.ErrNilClient
	}

	innerRing, err := cli.NeoFSAlphabetList()
	if err != nil {
		return 0, 0, err
	}

	return keyPosition(key, innerRing), int32(len(innerRing)), nil
}

// keyPosition returns "-1" if key is not found in the list, otherwise returns
// index of the key.
func keyPosition(key *ecdsa.PublicKey, list keys.PublicKeys) (result int32) {
	result = -1
	rawBytes := crypto.MarshalPublicKey(key)

	for i := range list {
		if bytes.Equal(list[i].Bytes(), rawBytes) {
			result = int32(i)
			break
		}
	}

	return result
}
