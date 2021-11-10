package netmap

import (
	"encoding/hex"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func genKey(t *testing.T) *keys.PrivateKey {
	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	return priv
}

func TestCleanupTable(t *testing.T) {
	infos := []netmap.NodeInfo{
		newNodeInfo(genKey(t).PublicKey()),
		newNodeInfo(genKey(t).PublicKey()),
		newNodeInfo(genKey(t).PublicKey()),
	}

	networkMap, err := netmap.NewNetmap(netmap.NodesFromInfo(infos))
	require.NoError(t, err)

	mapInfos := make(map[string][]byte)

	for i := range infos {
		binNodeInfo, err := infos[i].Marshal()
		require.NoError(t, err)

		mapInfos[hex.EncodeToString(infos[i].PublicKey())] = binNodeInfo
	}

	t.Run("update", func(t *testing.T) {
		c := newCleanupTable(true, 1)
		c.update(networkMap, 1)
		require.Len(t, c.lastAccess, len(infos))

		for k, v := range c.lastAccess {
			require.EqualValues(t, 1, v.epoch)
			require.False(t, v.removeFlag)

			_, ok := mapInfos[k]
			require.True(t, ok)
		}

		t.Run("update with flagged", func(t *testing.T) {
			key := hex.EncodeToString(infos[0].PublicKey())
			c.flag(key)

			c.update(networkMap, 2)
			require.EqualValues(t, 1, c.lastAccess[key].epoch)
			require.False(t, c.lastAccess[key].removeFlag)
		})
	})

	t.Run("touch", func(t *testing.T) {
		c := newCleanupTable(true, 1)
		c.update(networkMap, 1)

		key := hex.EncodeToString(infos[1].PublicKey())
		require.False(t, c.touch(key, 11, mapInfos[key]))
		require.EqualValues(t, 11, c.lastAccess[key].epoch)

		updNodeInfo := []byte("changed node info")

		require.True(t, c.touch(key, 11, updNodeInfo))
		require.EqualValues(t, 11, c.lastAccess[key].epoch)

		require.True(t, c.touch(key+"x", 12, updNodeInfo))
		require.EqualValues(t, 12, c.lastAccess[key+"x"].epoch)
	})

	t.Run("flag", func(t *testing.T) {
		c := newCleanupTable(true, 1)
		c.update(networkMap, 1)

		key := hex.EncodeToString(infos[1].PublicKey())
		c.flag(key)
		require.True(t, c.lastAccess[key].removeFlag)

		require.True(t, c.touch(key, 2, mapInfos[key]))
		require.False(t, c.lastAccess[key].removeFlag)
	})

	t.Run("iterator", func(t *testing.T) {
		c := newCleanupTable(true, 2)
		c.update(networkMap, 1)

		t.Run("no nodes to remove", func(t *testing.T) {
			cnt := 0
			require.NoError(t,
				c.forEachRemoveCandidate(2, func(_ string) error {
					cnt++
					return nil
				}))
			require.EqualValues(t, 0, cnt)
		})

		t.Run("all nodes to remove", func(t *testing.T) {
			cnt := 0
			require.NoError(t,
				c.forEachRemoveCandidate(4, func(s string) error {
					cnt++
					_, ok := mapInfos[s]
					require.True(t, ok)
					return nil
				}))
			require.EqualValues(t, len(infos), cnt)
		})

		t.Run("some nodes to remove", func(t *testing.T) {
			cnt := 0
			key := hex.EncodeToString(infos[1].PublicKey())

			require.True(t, c.touch(key, 4, mapInfos[key])) // one node was updated

			require.NoError(t,
				c.forEachRemoveCandidate(4, func(s string) error {
					cnt++
					require.NotEqual(t, s, key)
					return nil
				}))
			require.EqualValues(t, len(infos)-1, cnt)
		})
	})
}

func newNodeInfo(key *keys.PublicKey) (n netmap.NodeInfo) {
	n.SetPublicKey(key.Bytes())
	return n
}
