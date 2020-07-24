package placement

import (
	"github.com/gogo/protobuf/proto"
	"github.com/multiformats/go-multiaddr"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
)

// method returns copy of current Graph.
func (g *graph) copy() *graph {
	var (
		place *netmap.PlacementRule
		roots = make([]*netmap.Bucket, 0, len(g.roots))
		items = make([]netmapcore.Info, len(g.items))
	)

	copy(items, g.items)

	for _, root := range g.roots {
		var r *netmap.Bucket

		if root != nil {
			tmp := root.Copy()
			r = &tmp
		}

		roots = append(roots, r)
	}

	place = proto.Clone(g.place).(*netmap.PlacementRule)

	return &graph{
		roots: roots,
		items: items,
		place: place,
	}
}

func (g *graph) Exclude(list []multiaddr.Multiaddr) Graph {
	if len(list) == 0 {
		return g
	}

	var (
		sub    = g.copy()
		ignore = make([]uint32, 0, len(list))
	)

	for i := range list {
		for j := range sub.items {
			if list[i].String() == sub.items[j].Address() {
				ignore = append(ignore, uint32(j))
			}
		}
	}

	return sub.Filter(func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
		group.Exclude = ignore
		return bucket.GetMaxSelection(group)
	})
}

// Filter container by rules.
func (g *graph) Filter(rule FilterRule) Graph {
	if rule == nil {
		return g
	}

	var (
		sub   = g.copy()
		roots = make([]*netmap.Bucket, len(g.roots))
		items = make([]netmapcore.Info, len(g.items))
	)

	for i := range g.place.SFGroups {
		if g.roots[i] == nil {
			continue
		}

		root := g.roots[i].Copy()
		roots[i] = rule(g.place.SFGroups[i], &root)
	}

	copy(items, g.items)

	return &graph{
		roots: roots,
		items: items,
		place: sub.place,
	}
}

// NodeList returns slice of MultiAddresses for current graph.
func (g *graph) NodeList() ([]multiaddr.Multiaddr, error) {
	var (
		ln     = uint32(len(g.items))
		result = make([]multiaddr.Multiaddr, 0, ln)
		items  = make([]netmapcore.Info, len(g.items))
	)

	if ln == 0 {
		return nil, ErrEmptyNodes
	}

	copy(items, g.items)

	for _, root := range g.roots {
		if root == nil {
			continue
		}

		list := root.Nodelist()
		if len(list) == 0 {
			continue
		}

		for _, idx := range list {
			if ln <= idx.N {
				return nil, errors.Errorf("could not find index(%d) in list(size: %d)", ln, idx)
			}

			addr, err := multiaddr.NewMultiaddr(items[idx.N].Address())
			if err != nil {
				return nil, errors.Wrapf(err, "could not convert multi address(%s)", g.items[idx.N].Address())
			}

			result = append(result, addr)
		}
	}

	if len(result) == 0 {
		return nil, ErrEmptyNodes
	}

	return result, nil
}

// NodeInfo returns slice of NodeInfo for current graph.
func (g *graph) NodeInfo() ([]netmapcore.Info, error) {
	var (
		ln     = uint32(len(g.items))
		result = make([]netmapcore.Info, 0, ln)
		items  = make([]netmapcore.Info, len(g.items))
	)

	if ln == 0 {
		return nil, ErrEmptyNodes
	}

	copy(items, g.items)

	for _, root := range g.roots {
		if root == nil {
			continue
		}

		list := root.Nodelist()
		if len(list) == 0 {
			continue
		}

		for _, idx := range list {
			if ln <= idx.N {
				return nil, errors.Errorf("could not find index(%d) in list(size: %d)", ln, idx)
			}

			result = append(result, items[idx.N])
		}
	}

	if len(result) == 0 {
		return nil, ErrEmptyNodes
	}

	return result, nil
}
