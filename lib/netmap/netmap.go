package netmap

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"sort"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
)

type (
	// Bucket is an alias for github.com/nspcc-dev/netmap.Bucket
	Bucket = netmap.Bucket
	// SFGroup is an alias for github.com/nspcc-dev/netmap.SFGroup
	SFGroup = netmap.SFGroup
	// Select is an alias for github.com/nspcc-dev/netmap.Select
	Select = netmap.Select
	// Filter is an alias for github.com/nspcc-dev/netmap.Filter
	Filter = netmap.Filter
	// SimpleFilter is an alias for github.com/nspcc-dev/netmap.Filter
	SimpleFilter = netmap.SimpleFilter
	// PlacementRule is an alias for github.com/nspcc-dev/netmap.Filter
	PlacementRule = netmap.PlacementRule

	// NetMap is a general network map structure for NeoFS
	NetMap struct {
		mu    *sync.RWMutex
		root  Bucket
		items Nodes
	}

	// Nodes is an alias for slice of NodeInfo which is structure that describes every host
	Nodes []bootstrap.NodeInfo
)

const (
	// Separator separates key:value pairs in string representation of options.
	Separator = netmap.Separator

	// NodesBucket is the name for optionless bucket containing only nodes.
	NodesBucket = netmap.NodesBucket
)

var (
	// FilterIn returns filter, which checks if value is in specified list.
	FilterIn = netmap.FilterIn
	// FilterNotIn returns filter, which checks if value is not in specified list.
	FilterNotIn = netmap.FilterNotIn
	// FilterOR returns OR combination of filters.
	FilterOR = netmap.FilterOR
	// FilterAND returns AND combination of filters.
	FilterAND = netmap.FilterAND
	// FilterEQ returns filter, which checks if value is equal to v.
	FilterEQ = netmap.FilterEQ
	// FilterNE returns filter, which checks if value is not equal to v.
	FilterNE = netmap.FilterNE
	// FilterGT returns filter, which checks if value is greater than v.
	FilterGT = netmap.FilterGT
	// FilterGE returns filter, which checks if value is greater or equal than v.
	FilterGE = netmap.FilterGE
	// FilterLT returns filter, which checks if value is less than v.
	FilterLT = netmap.FilterLT
	// FilterLE returns filter, which checks if value is less or equal than v.
	FilterLE = netmap.FilterLE
)

var errNetMapsConflict = errors.New("netmaps are in conflict")

// Copy creates new slice of copied nodes.
func (n Nodes) Copy() Nodes {
	res := make(Nodes, len(n))
	for i := range n {
		res[i].Address = n[i].Address
		res[i].Status = n[i].Status

		if n[i].PubKey != nil {
			res[i].PubKey = make([]byte, len(n[i].PubKey))
			copy(res[i].PubKey, n[i].PubKey)
		}

		if n[i].Options != nil {
			res[i].Options = make([]string, len(n[i].Options))
			copy(res[i].Options, n[i].Options)
		}
	}

	return res
}

// NewNetmap is an constructor.
func NewNetmap() *NetMap {
	return &NetMap{
		items: make([]bootstrap.NodeInfo, 0),
		mu:    new(sync.RWMutex),
	}
}

// Equals return whether two netmap are identical.
func (n *NetMap) Equals(nm *NetMap) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.items) == len(nm.items) &&
		n.root.Equals(nm.root) &&
		reflect.DeepEqual(n.items, nm.items)
}

// Root returns netmap root-bucket.
func (n *NetMap) Root() *Bucket {
	n.mu.RLock()
	cp := n.root.Copy()
	n.mu.RUnlock()

	return &cp
}

// Copy creates and returns full copy of target netmap.
func (n *NetMap) Copy() *NetMap {
	n.mu.RLock()
	defer n.mu.RUnlock()

	nm := NewNetmap()
	nm.items = n.items.Copy()
	nm.root = n.root.Copy()

	return nm
}

type hashedItem struct {
	h    uint32
	info *bootstrap.NodeInfo
}

// Normalise reorders netmap items into some canonical order.
func (n *NetMap) Normalise() *NetMap {
	nm := NewNetmap()
	items := n.items.Copy()

	if len(items) == 0 {
		return nm
	}

	itemsH := make([]hashedItem, len(n.items))
	for i := range itemsH {
		itemsH[i].h = murmur3.Sum32(n.items[i].PubKey)
		itemsH[i].info = &items[i]
	}

	sort.Slice(itemsH, func(i, j int) bool {
		if itemsH[i].h == itemsH[j].h {
			return itemsH[i].info.Address < itemsH[j].info.Address
		}
		return itemsH[i].h < itemsH[j].h
	})

	lastHash := ^itemsH[0].h
	lastAddr := ""

	for i := range itemsH {
		if itemsH[i].h != lastHash || itemsH[i].info.Address != lastAddr {
			_ = nm.AddNode(itemsH[i].info)
			lastHash = itemsH[i].h
		}
	}

	return nm
}

// Hash returns hash of n.
func (n *NetMap) Hash() (sum [32]byte) {
	items := n.Normalise().Items()
	w := sha256.New()

	for i := range items {
		data, _ := items[i].Marshal()
		_, _ = w.Write(data)
	}

	s := w.Sum(nil)
	copy(sum[:], s)

	return
}

// InheritWeights calculates average capacity and minimal price, then provides buckets with IQR weight.
func (n *NetMap) InheritWeights() *NetMap {
	nm := n.Copy()

	// find average capacity in the network map
	meanCap := nm.root.Traverse(netmap.NewMeanAgg(), netmap.CapWeightFunc).Compute()
	capNorm := netmap.NewSigmoidNorm(meanCap)

	// find minimal price in the network map
	minPrice := nm.root.Traverse(netmap.NewMinAgg(), netmap.PriceWeightFunc).Compute()
	priceNorm := netmap.NewReverseMinNorm(minPrice)

	// provide all buckets with
	wf := netmap.NewWeightFunc(capNorm, priceNorm)
	meanAF := netmap.AggregatorFactory{New: netmap.NewMeanIQRAgg}
	nm.root.TraverseTree(meanAF, wf)

	return nm
}

// Merge checks if merge is possible and then add new elements from given netmap.
func (n *NetMap) Merge(n1 *NetMap) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	var (
		tr    = make(map[uint32]netmap.Node, len(n1.items))
		items = n.items
	)

loop:
	for j := range n1.items {
		for i := range n.items {
			if n.items[i].Equals(n1.items[j]) {
				tr[uint32(j)] = netmap.Node{
					N: uint32(i),
					C: n.items[i].Capacity(),
					P: n.items[i].Price(),
				}
				continue loop
			}
		}
		tr[uint32(j)] = netmap.Node{
			N: uint32(len(items)),
			C: n1.items[j].Capacity(),
			P: n1.items[j].Price(),
		}
		items = append(items, n1.items[j])
	}

	root := n1.root.UpdateIndices(tr)
	if n.root.CheckConflicts(root) {
		return errNetMapsConflict
	}

	n.items = items
	n.root.Merge(root)

	return nil
}

// FindGraph finds sub-graph filtered by given SFGroup.
func (n *NetMap) FindGraph(pivot []byte, ss ...SFGroup) (c *Bucket) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.root.FindGraph(pivot, ss...)
}

// FindNodes finds sub-graph filtered by given SFGroup and returns all sub-graph items.
func (n *NetMap) FindNodes(pivot []byte, ss ...SFGroup) (nodes []uint32) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.root.FindNodes(pivot, ss...).Nodes()
}

// Items return slice of all NodeInfo in netmap.
func (n *NetMap) Items() []bootstrap.NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.items
}

// ItemsCopy return copied slice of all NodeInfo in netmap (is it useful?).
func (n *NetMap) ItemsCopy() Nodes {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.items.Copy()
}

// Add adds node with given address and given options.
func (n *NetMap) Add(addr string, pk []byte, st bootstrap.NodeStatus, opts ...string) error {
	return n.AddNode(&bootstrap.NodeInfo{Address: addr, PubKey: pk, Status: st, Options: opts})
}

// Update replaces netmap with given netmap.
func (n *NetMap) Update(nxt *NetMap) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.root = nxt.root
	n.items = nxt.items
}

// GetMaxSelection returns 'maximal container' -- subgraph which contains
// any other subgraph satisfying specified selects and filters.
func (n *NetMap) GetMaxSelection(ss []Select, fs []Filter) (r *Bucket) {
	return n.root.GetMaxSelection(netmap.SFGroup{Selectors: ss, Filters: fs})
}

// AddNode adds to exited or new node slice of given options.
func (n *NetMap) AddNode(nodeInfo *bootstrap.NodeInfo, opts ...string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	info := *nodeInfo

	info.Options = append(info.Options, opts...)

	num := -1

	// looking for existed node info item
	for i := range n.items {
		if n.items[i].Equals(info) {
			num = i
			break
		}
	}
	// if item is not existed - add it
	if num < 0 {
		num = len(n.items)
		n.items = append(n.items, info)
	}

	return n.root.AddStrawNode(netmap.Node{
		N: uint32(num),
		C: n.items[num].Capacity(),
		P: n.items[num].Price(),
	}, info.Options...)
}

// GetNodesByOption returns slice of NodeInfo that has given option.
func (n *NetMap) GetNodesByOption(opts ...string) []bootstrap.NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	ns := n.root.GetNodesByOption(opts...)
	nodes := make([]bootstrap.NodeInfo, 0, len(ns))

	for _, info := range ns {
		nodes = append(nodes, n.items[info.N])
	}

	return nodes
}

// MarshalJSON custom marshaller.
func (n *NetMap) MarshalJSON() ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return json.Marshal(n.items)
}

// UnmarshalJSON custom unmarshaller.
func (n *NetMap) UnmarshalJSON(data []byte) error {
	var (
		nm    = NewNetmap()
		items []bootstrap.NodeInfo
	)

	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	for i := range items {
		if err := nm.Add(items[i].Address, items[i].PubKey, items[i].Status, items[i].Options...); err != nil {
			return err
		}
	}

	if n.mu == nil {
		n.mu = new(sync.RWMutex)
	}

	n.mu.Lock()
	n.root = nm.root
	n.items = nm.items
	n.mu.Unlock()

	return nil
}

// Size returns number of nodes in network map.
func (n *NetMap) Size() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.items)
}
