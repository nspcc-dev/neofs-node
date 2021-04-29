package invoke

import (
	"bytes"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// SideFeeProvider is an interface that used by invoker package method to
	// get extra fee for all non notary smart contract invocations in side chain.
	SideFeeProvider interface {
		SideChainFee() fixedn.Fixed8
	}

	// MainFeeProvider is an interface that used by invoker package method to
	// get extra fee for all non notary smart contract invocations in main chain.
	MainFeeProvider interface {
		MainChainFee() fixedn.Fixed8
	}
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

// AlphabetIndex returns index of the `key` in the alphabet key list from sidechain
// If key is not in the inner ring list, then returns `-1` as index.
func AlphabetIndex(cli *client.Client, key *ecdsa.PublicKey) (int32, error) {
	if cli == nil {
		return 0, client.ErrNilClient
	}

	alphabet, err := cli.Committee()
	if err != nil {
		return 0, err
	}

	return keyPosition(key, alphabet), nil
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
