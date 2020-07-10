package netmap

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/netmap"
	"github.com/stretchr/testify/require"
)

func TestNetMap_DataRace(t *testing.T) {
	var (
		nm    = NewNetmap()
		wg    = new(sync.WaitGroup)
		nodes = []bootstrap.NodeInfo{
			{Address: "SPB1", Options: []string{"/Location:Europe/Country:USA"}},
			{Address: "SPB2", Options: []string{"/Location:Europe/Country:Italy"}},
			{Address: "MSK1", Options: []string{"/Location:Europe/Country:Germany"}},
			{Address: "MSK2", Options: []string{"/Location:Europe/Country:Russia"}},
		}
	)

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			for _, node := range nodes {
				require.NoError(t, nm.Add(node.Address, node.PubKey, 0, node.Options...))
				// t.Logf("%02d: add node %q", n, node.Address)
			}

			wg.Done()
		}(i)
	}

	wg.Add(3 * 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			nm.Copy()
			// t.Logf("%02d: Copy", n)
			wg.Done()
		}(i)
		go func(n int) {
			nm.Items()
			// t.Logf("%02d: Items", n)
			wg.Done()
		}(i)
		go func(n int) {
			nm.Root()
			// t.Logf("%02d: Root", n)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestNetMapSuite(t *testing.T) {
	var (
		err   error
		nm1   = NewNetmap()
		nodes = []bootstrap.NodeInfo{
			{Address: "SPB1", Options: []string{"/Location:Europe/Country:USA"}, Status: 1},
			{Address: "SPB2", Options: []string{"/Location:Europe/Country:Italy"}, Status: 2},
			{Address: "MSK1", Options: []string{"/Location:Europe/Country:Germany"}, Status: 3},
			{Address: "MSK2", Options: []string{"/Location:Europe/Country:Russia"}, Status: 4},
		}
	)

	for _, node := range nodes {
		err = nm1.Add(node.Address, nil, node.Status, node.Options...)
		require.NoError(t, err)
	}

	t.Run("copy should work like expected", func(t *testing.T) {
		nm2 := nm1.Copy()
		require.Equal(t, nm1.root, nm2.root)
		require.Equal(t, nm1.items, nm2.items)
	})

	t.Run("add node should not ignore options", func(t *testing.T) {
		items := nm1.ItemsCopy()

		nm2 := NewNetmap()
		err = nm2.AddNode(&items[0], "/New/Option")
		require.NoError(t, err)
		require.Len(t, nm2.items, 1)
		require.Equal(t, append(items[0].Options, "/New/Option"), nm2.items[0].Options)
	})

	t.Run("copyItems should work like expected", func(t *testing.T) {
		require.Equal(t, nm1.items, nm1.ItemsCopy())
	})

	t.Run("marshal / unmarshal should be identical on same data", func(t *testing.T) {
		var nm2 *NetMap
		want, err := json.Marshal(nodes)
		require.NoError(t, err)

		actual, err := json.Marshal(nm1)
		require.NoError(t, err)

		require.Equal(t, want, actual)

		err = json.Unmarshal(actual, &nm2)
		require.NoError(t, err)
		require.Equal(t, nm1.root, nm2.root)
		require.Equal(t, nm1.items, nm2.items)
	})

	t.Run("unmarshal should override existing data", func(t *testing.T) {
		var nm2 *NetMap

		want, err := json.Marshal(nodes)
		require.NoError(t, err)

		actual, err := json.Marshal(nm1)
		require.NoError(t, err)

		require.Equal(t, want, actual)

		nm2 = nm1.Copy()
		err = nm2.Add("SOMEADDR", nil, 0, "/Location:Europe/Country:USA")
		require.NoError(t, err)

		err = json.Unmarshal(actual, &nm2)
		require.NoError(t, err)
		require.Equal(t, nm1.root, nm2.root)
		require.Equal(t, nm1.items, nm2.items)
	})

	t.Run("unmarshal should fail on bad data", func(t *testing.T) {
		var nm2 *NetMap
		require.Error(t, json.Unmarshal([]byte(`"some bad data"`), &nm2))
	})

	t.Run("unmarshal should fail on add nodes", func(t *testing.T) {
		var nm2 *NetMap
		require.Error(t, json.Unmarshal([]byte(`[{"address": "SPB1","options":["1-2-3-4"]}]`), &nm2))
	})

	t.Run("merge two netmaps", func(t *testing.T) {
		newNodes := []bootstrap.NodeInfo{
			{Address: "SPB3", Options: []string{"/Location:Europe/Country:France"}},
		}
		nm2 := NewNetmap()
		for _, node := range newNodes {
			err = nm2.Add(node.Address, nil, 0, node.Options...)
			require.NoError(t, err)
		}

		err = nm2.Merge(nm1)
		require.NoError(t, err)
		require.Len(t, nm2.items, len(nodes)+len(newNodes))

		ns := nm2.FindNodes([]byte("pivot"), netmap.SFGroup{
			Filters:   []Filter{{Key: "Country", F: FilterEQ("Germany")}},
			Selectors: []Select{{Count: 1, Key: NodesBucket}},
		})
		require.Len(t, ns, 1)
	})

	t.Run("weighted netmaps", func(t *testing.T) {
		strawNodes := []bootstrap.NodeInfo{
			{Address: "SPB2", Options: []string{"/Location:Europe/Country:Italy", "/Capacity:10", "/Price:100"}},
			{Address: "MSK1", Options: []string{"/Location:Europe/Country:Germany", "/Capacity:10", "/Price:1"}},
			{Address: "MSK2", Options: []string{"/Location:Europe/Country:Russia", "/Capacity:5", "/Price:10"}},
			{Address: "SPB1", Options: []string{"/Location:Europe/Country:France", "/Capacity:20", "/Price:2"}},
		}
		nm2 := NewNetmap()
		for _, node := range strawNodes {
			err = nm2.Add(node.Address, nil, 0, node.Options...)
			require.NoError(t, err)
		}

		ns1 := nm1.FindNodes([]byte("pivot"), netmap.SFGroup{
			Selectors: []Select{{Count: 2, Key: NodesBucket}},
		})
		require.Len(t, ns1, 2)

		ns2 := nm2.FindNodes([]byte("pivot"), netmap.SFGroup{
			Selectors: []Select{{Count: 2, Key: NodesBucket}},
		})
		require.Len(t, ns2, 2)
		require.NotEqual(t, ns1, ns2)
		require.Equal(t, []uint32{1, 3}, ns2)
	})
}

func TestNetMap_Normalise(t *testing.T) {
	const testCount = 5

	nodes := []bootstrap.NodeInfo{
		{Address: "SPB2", PubKey: []byte{4}, Options: []string{"/Location:Europe/Country:Italy", "/Capacity:10", "/Price:100"}},
		{Address: "MSK1", PubKey: []byte{2}, Options: []string{"/Location:Europe/Country:Germany", "/Capacity:10", "/Price:1"}},
		{Address: "MSK2", PubKey: []byte{3}, Options: []string{"/Location:Europe/Country:Russia", "/Capacity:5", "/Price:10"}},
		{Address: "SPB1", PubKey: []byte{1}, Options: []string{"/Location:Europe/Country:France", "/Capacity:20", "/Price:2"}},
	}

	add := func(nm *NetMap, indices ...int) {
		for _, i := range indices {
			err := nm.Add(nodes[i].Address, nodes[i].PubKey, 0, nodes[i].Options...)
			require.NoError(t, err)
		}
	}

	indices := []int{0, 1, 2, 3}

	nm1 := NewNetmap()
	add(nm1, indices...)
	norm := nm1.Normalise()

	for i := 0; i < testCount; i++ {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(indices), func(i, j int) { indices[i], indices[j] = indices[j], indices[i] })

		nm := NewNetmap()
		add(nm, indices...)
		require.Equal(t, norm, nm.Normalise())
	}

	t.Run("normalise removes duplicates", func(t *testing.T) {
		before := NewNetmap()
		add(before, indices...)
		before.items = append(before.items, before.items...)

		nm := before.Normalise()
		require.Len(t, nm.items, len(indices))

	loop:
		for i := range nodes {
			for j := range nm.items {
				if bytes.Equal(nm.items[j].PubKey, nodes[i].PubKey) {
					continue loop
				}
			}
			require.Fail(t, "normalized netmap does not contain '%s' node", nodes[i].Address)
		}
	})
}

func TestNodeInfo_Price(t *testing.T) {
	var info bootstrap.NodeInfo

	// too small value
	info = bootstrap.NodeInfo{Options: []string{"/Price:0.01048575"}}
	require.Equal(t, uint64(0), info.Price())

	// min value
	info = bootstrap.NodeInfo{Options: []string{"/Price:0.01048576"}}
	require.Equal(t, uint64(1), info.Price())

	// big value
	info = bootstrap.NodeInfo{Options: []string{"/Price:1000000000.666"}}
	require.Equal(t, uint64(1000000000.666*1e8/object.UnitsMB), info.Price())
}
