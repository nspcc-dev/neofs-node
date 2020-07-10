package placement

import (
	"crypto/ecdsa"
	"strconv"
	"testing"

	"bou.ke/monkey"
	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/stretchr/testify/require"
)

func testAddress(t *testing.T) multiaddr.Multiaddr {
	addr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	require.NoError(t, err)
	return addr
}

// -- -- //

func testPeerstore(t *testing.T) peers.Store {
	p, err := peers.NewStore(peers.StoreParams{
		Key:    test.DecodeKey(-1),
		Logger: test.NewTestLogger(false),
		Addr:   testAddress(t),
	})
	require.NoError(t, err)

	return p
}

const address = "/ip4/0.0.0.0/tcp/0/p2p/"

func TestPlacement_Neighbours(t *testing.T) {
	t.Run("Placement component NPE fix test", func(t *testing.T) {
		nodes := []bootstrap.NodeInfo{
			{Address: address + idFromString(t, "USA1"), Options: []string{"/Location:Europe/Country:USA/City:NewYork"}},
			{Address: address + idFromString(t, "ITL1"), Options: []string{"/Location:Europe/Country:Italy/City:Rome"}},
			{Address: address + idFromString(t, "RUS1"), Options: []string{"/Location:Europe/Country:Russia/City:SPB"}},
		}

		ps := testPeerstore(t)
		nm := testNetmap(t, nodes)

		p := New(Params{
			Log:       test.NewTestLogger(false),
			Peerstore: ps,
		})

		require.NotPanics(t, func() {
			require.NoError(t, p.Update(1, nm))
		})
	})

	t.Run("Placement Neighbours TestSuite", func(t *testing.T) {
		keys := []*ecdsa.PrivateKey{
			test.DecodeKey(0),
			test.DecodeKey(1),
			test.DecodeKey(2),
		}
		nodes := []bootstrap.NodeInfo{
			{
				Address: address + idFromString(t, "USA1"),
				PubKey:  crypto.MarshalPublicKey(&keys[0].PublicKey),
				Options: []string{"/Location:Europe/Country:USA/City:NewYork"},
			},
			{
				Address: address + idFromString(t, "ITL1"),
				PubKey:  crypto.MarshalPublicKey(&keys[1].PublicKey),
				Options: []string{"/Location:Europe/Country:Italy/City:Rome"},
			},
			{
				Address: address + idFromString(t, "RUS1"),
				PubKey:  crypto.MarshalPublicKey(&keys[2].PublicKey),
				Options: []string{"/Location:Europe/Country:Russia/City:SPB"},
			},
		}

		ps := testPeerstore(t)
		nm := testNetmap(t, nodes)

		p := New(Params{
			Log:       test.NewTestLogger(false),
			Netmap:    nm,
			Peerstore: ps,
		})

		t.Run("check, that items have expected length (< 30)", func(t *testing.T) {
			items := p.Neighbours(1, 0, false)
			require.Len(t, items, len(nm.ItemsCopy()))
		})

		t.Run("check, that items have expected length ( > 30)", func(t *testing.T) {
			opts := []string{"/Location:Europe/Country:Russia/City:SPB"}

			key, err := ps.GetPublicKey(ps.SelfID())
			require.NoError(t, err)

			keyBytes := crypto.MarshalPublicKey(key)

			addr := address + idFromString(t, "NewRUS")
			err = nm.Add(addr, keyBytes, 0, opts...)
			require.NoError(t, err)

			for i := 0; i < 30; i++ {
				addr := address + idFromString(t, "RUS"+strconv.Itoa(i+2))
				key := test.DecodeKey(i + len(nodes))
				pub := crypto.MarshalPublicKey(&key.PublicKey)
				err := nm.Add(addr, pub, 0, opts...)
				require.NoError(t, err)
			}

			ln := calculateCount(len(nm.ItemsCopy()))
			items := p.Neighbours(1, 0, false)
			require.Len(t, items, ln)
		})

		t.Run("check, that items is shuffled", func(t *testing.T) {
			var cur, pre []peers.ID
			for i := uint64(0); i < 10; i++ {
				cur = p.Neighbours(i, 0, false)
				require.NotEqual(t, pre, cur)

				pre = cur
			}
		})

		t.Run("check, that we can request more items that we have", func(t *testing.T) {
			require.NotPanics(t, func() {
				monkey.Patch(calculateCount, func(i int) int { return i + 1 })
				defer monkey.Unpatch(calculateCount)

				p.Neighbours(1, 0, false)
			})
		})
	})

	t.Run("unknown epoch", func(t *testing.T) {
		s := &placement{
			log:     test.NewTestLogger(false),
			nmStore: newNetMapStore(),
			ps:      testPeerstore(t),
		}

		require.Empty(t, s.Neighbours(1, 1, false))
	})

	t.Run("neighbors w/ set full flag", func(t *testing.T) {
		var (
			n          = 3
			e   uint64 = 5
			nm         = netmap.NewNetmap()
			nms        = newNetMapStore()
		)

		for i := 0; i < n; i++ {
			require.NoError(t, nm.Add("node"+strconv.Itoa(i), []byte{1}, 1))
		}

		nms.put(e, nm)

		s := &placement{
			log:     test.NewTestLogger(false),
			nmStore: nms,
			ps:      testPeerstore(t),
		}

		neighbors := s.Neighbours(1, e, true)

		require.Len(t, neighbors, n)
	})
}
