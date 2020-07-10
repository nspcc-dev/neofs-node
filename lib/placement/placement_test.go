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
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/refs"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/lib/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type (
	fakeDHT struct {
	}

	fakeContainerStorage struct {
		libcnr.Storage
		*sync.RWMutex
		items map[refs.CID]*container.Container
	}
)

var (
	testDHTCapacity = 100
)

// -- -- //

func testContainerStorage() *fakeContainerStorage {
	return &fakeContainerStorage{
		RWMutex: new(sync.RWMutex),
		items:   make(map[refs.CID]*container.Container, testDHTCapacity),
	}
}

func (f *fakeContainerStorage) GetContainer(p libcnr.GetParams) (*libcnr.GetResult, error) {
	f.RLock()
	val, ok := f.items[p.CID()]
	f.RUnlock()

	if !ok {
		return nil, errors.New("value for requested key not found in DHT")
	}

	res := new(libcnr.GetResult)
	res.SetContainer(val)

	return res, nil
}

func (f *fakeContainerStorage) Put(c *container.Container) error {
	id, err := c.ID()
	if err != nil {
		return err
	}
	f.Lock()
	f.items[id] = c
	f.Unlock()

	return nil
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

func testNetmap(t *testing.T, nodes []bootstrap.NodeInfo) *netmap.NetMap {
	nm := netmap.NewNetmap()

	for i := range nodes {
		err := nm.Add(nodes[i].Address, nil, 0, nodes[i].Options...)
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
		Log:       test.NewTestLogger(false),
		Netmap:    netmap.NewNetmap(),
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
	res1, err := container.New(100, owner, 0, netmap.PlacementRule{
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
	require.NoError(t, err)

	err = cnrStorage.Put(res1)
	require.NoError(t, err)

	res2, err := container.New(100, owner, 0, netmap.PlacementRule{
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
	require.NoError(t, err)

	err = cnrStorage.Put(res2)
	require.NoError(t, err)

	res3, err := container.New(100, owner, 0, netmap.PlacementRule{
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
	require.NoError(t, err)

	err = cnrStorage.Put(res3)
	require.NoError(t, err)

	t.Run("Should fail on empty container", func(t *testing.T) {
		id, err := res2.ID()
		require.NoError(t, err)
		_, err = p.Query(ctx, ContainerID(id))
		require.EqualError(t, errors.Cause(err), ErrEmptyContainer.Error())
	})

	t.Run("Should fail on Nodes Bucket is omitted in container", func(t *testing.T) {
		id, err := res3.ID()
		require.NoError(t, err)
		_, err = p.Query(ctx, ContainerID(id))
		require.EqualError(t, errors.Cause(err), ErrNodesBucketOmitted.Error())
	})

	t.Run("Should fail on unknown container (dht error)", func(t *testing.T) {
		_, err = p.Query(ctx, ContainerID(refs.CID{5}))
		require.Error(t, err)
	})

	id1, err := res1.ID()
	require.NoError(t, err)

	g, err := p.Query(ctx, ContainerID(id1))
	require.NoError(t, err)

	t.Run("Should return error on empty items", func(t *testing.T) {
		_, err = g.Filter(func(netmap.SFGroup, *netmap.Bucket) *netmap.Bucket {
			return &netmap.Bucket{}
		}).NodeList()
		require.EqualError(t, err, ErrEmptyNodes.Error())
	})

	t.Run("Should ignore some nodes", func(t *testing.T) {
		g1, err := p.Query(ctx, ContainerID(id1))
		require.NoError(t, err)

		expect, err := g1.
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		g2, err := p.Query(ctx, ContainerID(id1))
		require.NoError(t, err)

		actual, err := g2.
			Filter(filter).
			NodeList()
		require.NoError(t, err)

		require.Equal(t, expect, actual)

		g3, err := p.Query(ctx, ContainerID(id1))
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
			ContainerID(id1),
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
			cp.(*graph).items[i].Address = "BadAddress"
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
				netmap.NewNetmap(),
				rule,
				nil,
				refs.CID{},
			)
		})
	})
}
