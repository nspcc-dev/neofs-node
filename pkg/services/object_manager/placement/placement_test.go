package placement

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/refs"
	crypto "github.com/nspcc-dev/neofs-crypto"
	libcnr "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	testlogger "github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	fakeDHT struct {
	}

	fakeContainerStorage struct {
		storage.Storage
		*sync.RWMutex
		items map[refs.CID]*storage.Container
	}
)

var (
	testDHTCapacity = 100
)

// -- -- //

func testContainerStorage() *fakeContainerStorage {
	return &fakeContainerStorage{
		RWMutex: new(sync.RWMutex),
		items:   make(map[refs.CID]*storage.Container, testDHTCapacity),
	}
}

func (f *fakeContainerStorage) Get(cid storage.CID) (*storage.Container, error) {
	f.RLock()
	val, ok := f.items[cid]
	f.RUnlock()

	if !ok {
		return nil, errors.New("value for requested key not found in DHT")
	}

	return val, nil
}

func (f *fakeContainerStorage) Put(c *storage.Container) (*storage.CID, error) {
	id, err := libcnr.CalculateID(c)
	if err != nil {
		return nil, err
	}
	f.Lock()
	f.items[*id] = c
	f.Unlock()

	return id, nil
}

func (f *fakeDHT) UpdatePeers([]peers.ID) {
	// do nothing
}

func (f *fakeDHT) GetValue(ctx context.Context, key string) ([]byte, error) {
	panic("implement me")
}

func (f *fakeDHT) PutValue(ctx context.Context, key string, val []byte) error {
	panic("implement me")
}

func (f *fakeDHT) Get(ctx context.Context, key string) ([]byte, error) {
	panic("implement me")
}

func (f *fakeDHT) Put(ctx context.Context, key string, val []byte) error {
	panic("implement me")
}

// -- -- //

func testNetmap(t *testing.T, nodes []bootstrap.NodeInfo) *NetMap {
	nm := netmapcore.New()

	for i := range nodes {
		info := netmapcore.Info{}
		info.SetAddress(nodes[i].Address)
		info.SetOptions(nodes[i].Options)
		info.SetPublicKey(crypto.MarshalPublicKey(&test.DecodeKey(i).PublicKey))
		err := nm.AddNode(info)
		require.NoError(t, err)
	}

	return nm
}

// -- -- //

func idFromString(t *testing.T, id string) string {
	buf, err := multihash.Encode([]byte(id), multihash.ID)
	require.NoError(t, err)

	return (multihash.Multihash(buf)).B58String()
}

func idFromAddress(t *testing.T, addr multiaddr.Multiaddr) string {
	id, err := addr.ValueForProtocol(multiaddr.P_P2P)
	require.NoError(t, err)

	buf, err := base58.Decode(id)
	require.NoError(t, err)

	hs, err := multihash.Decode(buf)
	require.NoError(t, err)

	return string(hs.Digest)
}

// -- -- //

func TestPlacement(t *testing.T) {
	multiaddr.SwapToP2pMultiaddrs()
	testAddress := "/ip4/0.0.0.0/tcp/0/p2p/"
	key := test.DecodeKey(-1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ids := map[string]struct{}{
		"GRM1": {}, "GRM2": {}, "GRM3": {}, "GRM4": {},
		"SPN1": {}, "SPN2": {}, "SPN3": {}, "SPN4": {},
	}

	nodes := []bootstrap.NodeInfo{
		{Address: testAddress + idFromString(t, "USA1"), Options: []string{"/Location:Europe/Country:USA/City:NewYork"}},
		{Address: testAddress + idFromString(t, "ITL1"), Options: []string{"/Location:Europe/Country:Italy/City:Rome"}},
		{Address: testAddress + idFromString(t, "RUS1"), Options: []string{"/Location:Europe/Country:Russia/City:SPB"}},
	}

	for id := range ids {
		var opts []string
		switch {
		case strings.Contains(id, "GRM"):
			opts = append(opts, "/Location:Europe/Country:Germany/City:"+id)
		case strings.Contains(id, "SPN"):
			opts = append(opts, "/Location:Europe/Country:Spain/City:"+id)
		}

		for i := 0; i < 4; i++ {
			id := id + strconv.Itoa(i)

			nodes = append(nodes, bootstrap.NodeInfo{
				Address: testAddress + idFromString(t, id),
				Options: opts,
			})
		}
	}

	sort.Slice(nodes, func(i, j int) bool {
		return strings.Compare(nodes[i].Address, nodes[j].Address) == -1
	})

	nm := testNetmap(t, nodes)

	cnrStorage := testContainerStorage()

	p := New(Params{
		Log:       testlogger.NewLogger(false),
		Peerstore: testPeerstore(t),
		Fetcher:   cnrStorage,
	})

	require.NoError(t, p.Update(1, nm))

	oid, err := refs.NewObjectID()
	require.NoError(t, err)

	// filter over oid
	filter := func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
		return bucket.GetSelection(group.Selectors, oid[:])
	}

	owner, err := refs.NewOwnerID(&key.PublicKey)
	require.NoError(t, err)

	cnr1 := new(storage.Container)
	cnr1.SetOwnerID(owner)
	cnr1.SetBasicACL(basic.FromUint32(0))
	cnr1.SetPlacementRule(netmap.PlacementRule{
		ReplFactor: 2,
		SFGroups: []netmap.SFGroup{
			{
				Selectors: []netmap.Select{
					{Key: "Country", Count: 1},
					{Key: "City", Count: 2},
					{Key: netmap.NodesBucket, Count: 1},
				},
				Filters: []netmap.Filter{
					{Key: "Country", F: netmap.FilterIn("Germany", "Spain")},
				},
			},
		},
	})

	cid1, err := cnrStorage.Put(cnr1)
	require.NoError(t, err)

	cnr2 := new(storage.Container)
	cnr2.SetOwnerID(owner)
	cnr2.SetBasicACL(basic.FromUint32(0))
	cnr2.SetPlacementRule(netmap.PlacementRule{
		ReplFactor: 2,
		SFGroups: []netmap.SFGroup{
			{
				Selectors: []netmap.Select{
					{Key: "Country", Count: 1},
					{Key: netmap.NodesBucket, Count: 10},
				},
				Filters: []netmap.Filter{
					{Key: "Country", F: netmap.FilterIn("Germany", "Spain")},
				},
			},
		},
	})

	cid2, err := cnrStorage.Put(cnr2)
	require.NoError(t, err)

	cnr3 := new(storage.Container)
	cnr3.SetOwnerID(owner)
	cnr3.SetBasicACL(basic.FromUint32(0))
	cnr3.SetPlacementRule(netmap.PlacementRule{
		ReplFactor: 2,
		SFGroups: []netmap.SFGroup{
			{
				Selectors: []netmap.Select{
					{Key: "Country", Count: 1},
				},
				Filters: []netmap.Filter{
					{Key: "Country", F: netmap.FilterIn("Germany", "Spain")},
				},
			},
		},
	})

	cid3, err := cnrStorage.Put(cnr3)
	require.NoError(t, err)

	t.Run("Should fail on empty container", func(t *testing.T) {
		_, err = p.Query(ctx, ContainerID(*cid2))
		require.EqualError(t, errors.Cause(err), ErrEmptyContainer.Error())
	})

	t.Run("Should fail on Nodes Bucket is omitted in container", func(t *testing.T) {
		_, err = p.Query(ctx, ContainerID(*cid3))
		require.EqualError(t, errors.Cause(err), ErrNodesBucketOmitted.Error())
	})

	t.Run("Should fail on unknown container (dht error)", func(t *testing.T) {
		_, err = p.Query(ctx, ContainerID(refs.CID{5}))
		require.Error(t, err)
	})

	g, err := p.Query(ctx, ContainerID(*cid1))
	require.NoError(t, err)

	t.Run("Should return error on empty items", func(t *testing.T) {
		_, err = g.Filter(func(netmap.SFGroup, *netmap.Bucket) *netmap.Bucket {
			return &netmap.Bucket{}
		}).NodeList()
		require.EqualError(t, err, ErrEmptyNodes.Error())
	})

	t.Run("Should ignore some nodes", func(t *testing.T) {
		g1, err := p.Query(ctx, ContainerID(*cid1))
		require.NoError(t, err)

		expect, err := g1.
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		g2, err := p.Query(ctx, ContainerID(*cid1))
		require.NoError(t, err)

		actual, err := g2.
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		require.Equal(t, expect, actual)

		g3, err := p.Query(ctx, ContainerID(*cid1))
		require.NoError(t, err)

		actual, err = g3.
			Exclude(expect).
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		for _, item := range expect {
			require.NotContains(t, actual, item)
		}

		g4, err := p.Query(ctx,
			ContainerID(*cid1),
			ExcludeNodes(expect))
		require.NoError(t, err)

		actual, err = g4.
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		for _, item := range expect {
			require.NotContains(t, actual, item)
		}
	})

	t.Run("Should return error on nil Buckets", func(t *testing.T) {
		_, err = g.Filter(func(netmap.SFGroup, *netmap.Bucket) *netmap.Bucket {
			return nil
		}).NodeList()
		require.EqualError(t, err, ErrEmptyNodes.Error())
	})

	t.Run("Should return error on empty NodeInfo's", func(t *testing.T) {
		cp := g.Filter(func(netmap.SFGroup, *netmap.Bucket) *netmap.Bucket {
			return nil
		})

		cp.(*graph).items = nil

		_, err := cp.NodeList()
		require.EqualError(t, err, ErrEmptyNodes.Error())
	})

	t.Run("Should return error on unknown items", func(t *testing.T) {
		cp := g.Filter(func(_ netmap.SFGroup, b *netmap.Bucket) *netmap.Bucket {
			return b
		})

		cp.(*graph).items = cp.(*graph).items[:5]

		_, err := cp.NodeList()
		require.Error(t, err)
	})

	t.Run("Should return error on bad items", func(t *testing.T) {
		cp := g.Filter(func(_ netmap.SFGroup, b *netmap.Bucket) *netmap.Bucket {
			return b
		})

		for i := range cp.(*graph).items {
			cp.(*graph).items[i].SetAddress("BadAddress")
		}

		_, err := cp.NodeList()
		require.EqualError(t, errors.Cause(err), "failed to parse multiaddr \"BadAddress\": must begin with /")
	})

	list, err := g.
		Filter(filter).
		// must return same graph on empty filter
		Filter(nil).
		NodeList()
	require.NoError(t, err)

	// 1 Country, 2 Cities, 1 Node = 2 Nodes
	require.Len(t, list, 2)
	for _, item := range list {
		id := idFromAddress(t, item)
		require.Contains(t, ids, id[:4]) // exclude our postfix (0-4)
	}
}

func TestContainerGraph(t *testing.T) {
	t.Run("selectors index out-of-range", func(t *testing.T) {
		rule := new(netmap.PlacementRule)

		rule.SFGroups = append(rule.SFGroups, netmap.SFGroup{})

		require.NotPanics(t, func() {
			_, _ = ContainerGraph(
				netmapcore.New(),
				rule,
				nil,
				refs.CID{},
			)
		})
	})
}
