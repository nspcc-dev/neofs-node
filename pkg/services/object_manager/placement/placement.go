package placement

import (
	"bytes"
	"context"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/refs"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	libnetmap "github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const defaultChronologyDuration = 1

var (
	// ErrEmptyNodes when container doesn't contains any nodes
	ErrEmptyNodes = errors.New("container doesn't contains nodes")

	// ErrNodesBucketOmitted when in PlacementRule, Selector has not NodesBucket
	ErrNodesBucketOmitted = errors.New("nodes-bucket is omitted")

	// ErrEmptyContainer when GetMaxSelection or GetSelection returns empty result
	ErrEmptyContainer = errors.New("could not get container, it's empty")
)

var errNilNetMap = errors.New("network map is nil")

// New is a placement component constructor.
func New(p Params) Component {
	if p.Netmap == nil {
		p.Netmap = netmap.New()
	}

	if p.ChronologyDuration <= 0 {
		p.ChronologyDuration = defaultChronologyDuration
	}

	pl := &placement{
		log: p.Log,
		cnr: p.Fetcher,

		chronologyDur: p.ChronologyDuration,
		nmStore:       newNetMapStore(),

		ps: p.Peerstore,

		healthy: atomic.NewBool(false),
	}

	pl.nmStore.put(0, p.Netmap)

	return pl
}

func (p *placement) Name() string  { return "PresentInNetwork" }
func (p *placement) Healthy() bool { return p.healthy.Load() }

type strNodes []netmap.Info

func (n strNodes) String() string {
	list := make([]string, 0, len(n))
	for i := range n {
		list = append(list, n[i].Address())
	}

	return `[` + strings.Join(list, ",") + `]`
}

func (p *placement) Update(epoch uint64, nm *NetMap) error {
	cnm := p.nmStore.get(p.nmStore.epoch())
	if cnm == nil {
		return errNilNetMap
	}

	items := nm.Nodes()

	p.log.Debug("update to new netmap",
		zap.Stringer("nodes", strNodes(items)))

	p.log.Debug("update peerstore")

	if err := p.ps.Update(nm); err != nil {
		return err
	}

	var (
		pubkeyBinary []byte
		healthy      bool
	)

	// storage nodes must be presented in network map to be healthy
	pubkey, err := p.ps.GetPublicKey(p.ps.SelfID())
	if err != nil {
		p.log.Error("can't get my own public key")
	}

	pubkeyBinary = crypto.MarshalPublicKey(pubkey)

	for i := range items {
		if bytes.Equal(pubkeyBinary, items[i].PublicKey()) {
			healthy = true
		}

		p.log.Debug("new peer for dht",
			zap.Stringer("peer", peers.IDFromBinary(items[i].PublicKey())),
			zap.String("addr", items[i].Address()))
	}

	// make copy to previous
	p.log.Debug("update previous netmap")

	if epoch > p.chronologyDur {
		p.nmStore.trim(epoch - p.chronologyDur)
	}

	p.log.Debug("update current netmap")
	p.nmStore.put(epoch, nm)

	p.log.Debug("update current epoch")

	p.healthy.Store(healthy)

	return nil
}

// NetworkState returns copy of current NetworkMap.
func (p *placement) NetworkState() *bootstrap.SpreadMap {
	ns := p.networkState(p.nmStore.epoch())
	if ns == nil {
		ns = &networkState{nm: netmap.New()}
	}

	nodes := ns.nm.Nodes()

	res := &bootstrap.SpreadMap{
		Epoch:  ns.epoch,
		NetMap: make([]bootstrap.NodeInfo, 0, len(nodes)),
	}

	for i := range nodes {
		res.NetMap = append(res.NetMap, bootstrap.NodeInfo{
			Address: nodes[i].Address(),
			PubKey:  nodes[i].PublicKey(),
			Options: nodes[i].Options(),
		})
	}

	return res
}

func (p *placement) networkState(epoch uint64) *networkState {
	nm := p.nmStore.get(epoch)
	if nm == nil {
		return nil
	}

	return &networkState{
		nm:    nm,
		epoch: epoch,
	}
}

// Query returns graph based on container.
func (p *placement) Query(ctx context.Context, opts ...QueryOption) (Graph, error) {
	var (
		query  QueryOptions
		ignore []uint32
	)

	for _, opt := range opts {
		opt(&query)
	}

	epoch := p.nmStore.epoch()
	if query.Previous > 0 {
		epoch -= uint64(query.Previous)
	}

	state := p.networkState(epoch)
	if state == nil {
		return nil, errors.Errorf("could not get network state for epoch #%d", epoch)
	}

	items := state.nm.Nodes()

	cnr, err := p.cnr.Get(query.CID)
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch container")
	}

	for i := range query.Excludes {
		for j := range items {
			if query.Excludes[i].String() == items[j].Address() {
				ignore = append(ignore, uint32(j))
			}
		}
	}

	rule := cnr.PlacementRule()

	return ContainerGraph(state.nm, &rule, ignore, query.CID)
}

// ContainerGraph applies the placement rules to network map and returns container graph.
func ContainerGraph(nm *NetMap, rule *libnetmap.PlacementRule, ignore []uint32, cid refs.CID) (Graph, error) {
	root := nm.Root()
	roots := make([]*netmap.Bucket, 0, len(rule.SFGroups))

	for i := range rule.SFGroups {
		rule.SFGroups[i].Exclude = ignore
		if ln := len(rule.SFGroups[i].Selectors); ln <= 0 ||
			rule.SFGroups[i].Selectors[ln-1].Key != libnetmap.NodesBucket {
			return nil, errors.Wrapf(ErrNodesBucketOmitted, "container (%s)", cid)
		}

		bigSelectors := make([]libnetmap.Select, len(rule.SFGroups[i].Selectors))
		for j := range rule.SFGroups[i].Selectors {
			bigSelectors[j] = libnetmap.Select{
				Key:   rule.SFGroups[i].Selectors[j].Key,
				Count: rule.SFGroups[i].Selectors[j].Count,
			}

			if rule.ReplFactor > 1 && rule.SFGroups[i].Selectors[j].Key == libnetmap.NodesBucket {
				bigSelectors[j].Count *= rule.ReplFactor
			}
		}

		sf := libnetmap.SFGroup{
			Selectors: bigSelectors,
			Filters:   rule.SFGroups[i].Filters,
			Exclude:   ignore,
		}

		if tree := root.Copy().GetMaxSelection(sf); tree != nil {
			// fetch graph for replication factor seeded by ContainerID
			if tree = tree.GetSelection(bigSelectors, cid[:]); tree == nil {
				return nil, errors.Wrapf(ErrEmptyContainer, "for container(%s) with repl-factor(%d)",
					cid, rule.ReplFactor)
			}

			roots = append(roots, tree)

			continue
		}

		return nil, errors.Wrap(ErrEmptyContainer, "empty for bigSelector")
	}

	return &graph{
		roots: roots,
		items: nm.Nodes(),
		place: rule,
	}, nil
}
