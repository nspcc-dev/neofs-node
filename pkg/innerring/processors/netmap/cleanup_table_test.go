package netmap

import (
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

	var networkMap netmap.NetMap
	networkMap.SetNodes(infos)

	mapInfos := make(map[string][]byte)

	for i := range infos {
		binNodeInfo := infos[i].Marshal()

		mapInfos[netmap.StringifyPublicKey(infos[i])] = binNodeInfo
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
			key := netmap.StringifyPublicKey(infos[0])
			c.flag(key, 1)

			c.update(networkMap, 2)
			require.EqualValues(t, 1, c.lastAccess[key].epoch)
			require.False(t, c.lastAccess[key].removeFlag)
		})
	})

	t.Run("touch", func(t *testing.T) {
		c := newCleanupTable(true, 1)
		c.update(networkMap, 1)

		key := netmap.StringifyPublicKey(infos[1])
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

		key := netmap.StringifyPublicKey(infos[1])
		c.flag(key, 1)
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
			key := netmap.StringifyPublicKey(infos[1])

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
