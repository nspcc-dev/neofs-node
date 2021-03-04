package governance

import (
	"sort"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/stretchr/testify/require"
)

func TestNewAlphabetList(t *testing.T) {
	k, err := generateKeys(14)
	require.NoError(t, err)

	orig := keys.PublicKeys{k[0], k[1], k[2], k[3], k[4], k[5], k[6]}

	t.Run("same keys", func(t *testing.T) {
		list, err := newAlphabetList(orig, orig)
		require.NoError(t, err)
		require.Nil(t, list)
	})

	t.Run("not enough mainnet keys", func(t *testing.T) {
		_, err := newAlphabetList(orig, orig[:len(orig)-1])
		require.Error(t, err)
	})

	t.Run("less than third new keys", func(t *testing.T) {
		exp := keys.PublicKeys{k[1], k[2], k[3], k[4], k[5], k[6], k[7]}
		got, err := newAlphabetList(orig, exp)
		require.NoError(t, err)
		require.True(t, equalPublicKeyLists(exp, got))
	})

	t.Run("completely new list of keys", func(t *testing.T) {
		list := orig
		exp := keys.PublicKeys{k[7], k[8], k[9], k[10], k[11], k[12], k[13]}

		rounds := []keys.PublicKeys{
			{k[0], k[1], k[2], k[3], k[4], k[7], k[8]},
			{k[0], k[1], k[2], k[7], k[8], k[9], k[10]},
			{k[0], k[7], k[8], k[9], k[10], k[11], k[12]},
			exp,
		}
		ln := len(rounds)

		for i := 0; i < ln; i++ {
			list, err = newAlphabetList(list, exp)
			require.NoError(t, err)
			require.True(t, equalPublicKeyLists(list, rounds[i]))
		}
	})
}

func generateKeys(n int) (keys.PublicKeys, error) {
	pubKeys := make(keys.PublicKeys, 0, n)

	for i := 0; i < n; i++ {
		privKey, err := keys.NewPrivateKey()
		if err != nil {
			return nil, err
		}

		pubKeys = append(pubKeys, privKey.PublicKey())
	}

	sort.Sort(pubKeys)

	return pubKeys, nil
}

func equalPublicKeyLists(a, b keys.PublicKeys) bool {
	if len(a) != len(b) {
		return false
	}

	for i, node := range a {
		if !b[i].Equal(node) {
			return false
		}
	}

	return true
}
