package peers

import (
	"strconv"
	"testing"

	"github.com/multiformats/go-multiaddr"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testSign struct {
	ID   ID
	Sign []byte
}

const debug = false

func createNetworkMap(t *testing.T) *netmap.NetMap {
	var (
		Region  = []string{"America", "Europe", "Asia"}
		Country = map[string][]string{
			"America": {"USA", "Canada", "Brazil"},
			"Europe":  {"France", "Germany", "Sweden"},
			"Asia":    {"Russia", "China", "Korea", "Japan"},
		}
		City = map[string][]string{
			"USA":     {"Washington", "New-York", "Seattle", "Chicago", "Detroit"},
			"Canada":  {"Toronto", "Ottawa", "Quebec", "Winnipeg"},
			"Brazil":  {"Rio-de-Janeiro", "San-Paulo", "Salvador"},
			"France":  {"Paris", "Lion", "Nice", "Marseille"},
			"Germany": {"Berlin", "Munich", "Dortmund", "Hamburg", "Cologne"},
			"Sweden":  {"Stockholm", "Malmo", "Uppsala"},
			"Russia":  {"Moscow", "Saint-Petersburg", "Ekaterinburg", "Novosibirsk"},
			"China":   {"Beijing", "Shanghai", "Shenzhen", "Guangzhou"},
			"Korea":   {"Seoul", "Busan"},
			"Japan":   {"Tokyo", "Kyoto", "Yokohama", "Osaka"},
		}
		nm         = netmap.NewNetmap()
		port int64 = 4000
		i          = 0
	)
	for _, r := range Region {
		for _, co := range Country[r] {
			for _, ci := range City[co] {
				addr := "/ip4/127.0.0.1/tcp/" + strconv.FormatInt(port, 10)
				port++
				option := "/Region:" + r + "/Country:" + co + "/City:" + ci
				pk := crypto.MarshalPublicKey(&test.DecodeKey(i).PublicKey)
				i++

				require.NoError(t, nm.Add(addr, pk, 0, option))
			}
		}
	}
	return nm
}

func testMulatiAddress(t *testing.T) multiaddr.Multiaddr {
	addr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	require.NoError(t, err)
	return addr
}

func TestPeerstore(t *testing.T) {
	var (
		l   = test.NewTestLogger(debug)
		key = test.DecodeKey(1)
	)

	t.Run("it should creates new store", func(t *testing.T) {
		ps, err := NewStore(StoreParams{
			Key:    key,
			Logger: l,
			Addr:   testMulatiAddress(t),
		})
		require.NoError(t, err)
		require.NotNil(t, ps)

		maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4000")
		require.NoError(t, err)

		expect := crypto.MarshalPublicKey(&key.PublicKey)

		id, err := ps.AddPeer(maddr, &key.PublicKey, key)
		require.NoError(t, err)

		pub, err := ps.GetPublicKey(id)
		require.NoError(t, err)

		actual := crypto.MarshalPublicKey(pub)
		require.Equal(t, expect, actual)

		addr1, err := ps.GetAddr(id)
		require.NoError(t, err)
		require.True(t, maddr.Equal(addr1))

		ps.DeletePeer(id)
		addr1, err = ps.GetAddr(id)
		require.Nil(t, addr1)
		require.Error(t, err)
	})

	t.Run("it should creates new store based on netmap", func(t *testing.T) {
		var nm = createNetworkMap(t)

		ps, err := NewStore(StoreParams{
			Key:    key,
			Logger: l,
			Addr:   testMulatiAddress(t),
		})
		require.NoError(t, err)
		require.NotNil(t, ps)

		err = ps.Update(nm)
		require.NoError(t, err)

		expect := nm.Items()[0].PubKey

		id := IDFromBinary(expect)

		addr, err := ps.GetAddr(id)
		require.NoError(t, err)
		require.Equal(t, nm.Items()[0].Address, addr.String())

		pub, err := ps.GetPublicKey(id)
		require.NoError(t, err)

		actual := crypto.MarshalPublicKey(pub)
		require.Equal(t, expect, actual)
	})

	t.Run("multiple store's", func(t *testing.T) {
		var (
			count = 10
			items = make([]Store, 0, count)

			data  = []byte("Hello world")
			peers = make([]Peer, 0, count)
			signs = make([]*testSign, 0, count)
		)

		for i := 0; i < count; i++ {
			key := test.DecodeKey(i)
			addr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
			require.NoError(t, err)

			peers = append(peers, NewLocalPeer(addr, key))
		}

		for i := 0; i < count; i++ {
			key, err := peers[i].PrivateKey()
			require.NoError(t, err)

			store, err := NewStore(StoreParams{
				Addr:   peers[i].Address(),
				Key:    key,
				Logger: zap.L(),
			})
			require.NoError(t, err)

			items = append(items, store)

			hash, err := store.Sign(data)
			require.NoError(t, err)

			sign := &testSign{
				ID:   peers[i].ID(),
				Sign: hash,
			}
			signs = append(signs, sign)
			l.Info("add peer",
				zap.Stringer("id", peers[i].ID()))
		}

		for i := 0; i < count; i++ {
			signature, err := items[i].Sign(data)
			require.NoError(t, err)

			// check the newly generated signature
			err = items[i].Verify(peers[i].ID(), data, signature)
			require.NoError(t, err)

			for j := 0; j < count; j++ {
				// check previously generated signature
				addr, pub := peers[j].Address(), peers[j].PublicKey()
				key, err := peers[j].PrivateKey()
				require.NoError(t, err)

				_, err = items[i].AddPeer(addr, pub, key)
				require.NoError(t, err)

				err = items[i].Verify(signs[j].ID, data, signs[j].Sign)
				require.NoError(t, err)
			}
		}
	})

	t.Run("Get self address", func(t *testing.T) {
		addr := testMulatiAddress(t)

		ps, err := NewStore(StoreParams{
			Key:    key,
			Logger: l,
			Addr:   addr,
		})
		require.NoError(t, err)
		require.NotNil(t, ps)

		selfAddr, err := ps.GetAddr(ps.SelfID())
		require.NoError(t, err)
		require.Equal(t, selfAddr, addr)
	})

	t.Run("Get ID for multi address", func(t *testing.T) {
		addr := testMulatiAddress(t)

		ps, err := NewStore(StoreParams{
			Key:    key,
			Logger: l,
			Addr:   addr,
		})
		require.NoError(t, err)
		require.NotNil(t, ps)

		maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4000")
		require.NoError(t, err)

		id, err := ps.AddPeer(maddr, &key.PublicKey, key)
		require.NoError(t, err)

		res, err := ps.AddressID(maddr)
		require.NoError(t, err)
		require.True(t, id.Equal(res))

		maddr2, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/4001")
		require.NoError(t, err)

		res, err = ps.AddressID(maddr2)
		require.EqualError(t, err, errPeerNotFound.Error())
	})
}
