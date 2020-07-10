package replication

import (
	"context"
	"sync"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/nspcc-dev/neofs-node/lib/placement"
	"github.com/nspcc-dev/neofs-node/lib/rand"
	"github.com/pkg/errors"
)

type (
	replicationScheduler struct {
		cac ContainerActualityChecker
		ls  localstore.Iterator
	}

	// SchedulerParams groups the parameters of scheduler constructor.
	SchedulerParams struct {
		ContainerActualityChecker
		localstore.Iterator
	}

	objectPool struct {
		mu    *sync.Mutex
		tasks []Address
	}

	multiSolver struct {
		as AddressStore
		pl placement.Component
	}

	// MultiSolverParams groups the parameters of multi solver constructor.
	MultiSolverParams struct {
		AddressStore
		Placement placement.Component
	}
)

const (
	errPoolExhausted = internal.Error("object pool is exhausted")

	objectPoolInstanceFailMsg = "could not create object pool"
	errEmptyLister            = internal.Error("empty local objects lister")
	errEmptyContainerActual   = internal.Error("empty container actuality checker")

	multiSolverInstanceFailMsg = "could not create multi solver"
	errEmptyAddressStore       = internal.Error("empty address store")
	errEmptyPlacement          = internal.Error("empty placement")
	replicationSchedulerEntity = "replication scheduler"
)

// NewObjectPool is an object pool constructor.
func NewObjectPool() ObjectPool {
	return &objectPool{mu: new(sync.Mutex)}
}

// NewReplicationScheduler is a replication scheduler constructor.
func NewReplicationScheduler(p SchedulerParams) (Scheduler, error) {
	switch {
	case p.ContainerActualityChecker == nil:
		return nil, errors.Wrap(errEmptyContainerActual, objectPoolInstanceFailMsg)
	case p.Iterator == nil:
		return nil, errors.Wrap(errEmptyLister, objectPoolInstanceFailMsg)
	}

	return &replicationScheduler{
		cac: p.ContainerActualityChecker,
		ls:  p.Iterator,
	}, nil
}

// NewMultiSolver is a multi solver constructor.
func NewMultiSolver(p MultiSolverParams) (MultiSolver, error) {
	switch {
	case p.Placement == nil:
		return nil, errors.Wrap(errEmptyPlacement, multiSolverInstanceFailMsg)
	case p.AddressStore == nil:
		return nil, errors.Wrap(errEmptyAddressStore, multiSolverInstanceFailMsg)
	}

	return &multiSolver{
		as: p.AddressStore,
		pl: p.Placement,
	}, nil
}

func (s *objectPool) Update(pool []Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks = pool
}

func (s *objectPool) Undone() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.tasks)
}

func (s *objectPool) Pop() (Address, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.tasks) == 0 {
		return Address{}, errPoolExhausted
	}

	head := s.tasks[0]
	s.tasks = s.tasks[1:]

	return head, nil
}

func (s *replicationScheduler) SelectForReplication(limit int) ([]Address, error) {
	// Attention! This routine might be inefficient with big number of objects
	// and containers. Consider using fast traversal and filtering algorithms
	// with sieve of bloom filters.
	migration := make([]Address, 0, limit)
	replication := make([]Address, 0)
	ctx := context.Background()

	if err := s.ls.Iterate(nil, func(meta *localstore.ObjectMeta) bool {
		if s.cac.Actual(ctx, meta.Object.SystemHeader.CID) {
			replication = append(replication, *meta.Object.Address())
		} else {
			migration = append(migration, *meta.Object.Address())
		}
		return len(migration) >= limit
	}); err != nil {
		return nil, err
	}

	lnM := len(migration)
	lnR := len(replication)
	edge := 0

	// I considered using rand.Perm() and appending elements in `for` cycle.
	// But it seems, that shuffling is efficient even when `limit-lnM`
	// is 1000 times smaller than `lnR`. But it can be discussed and changed
	// later anyway.
	if lnM < limit {
		r := rand.New()
		r.Shuffle(lnR, func(i, j int) {
			replication[i], replication[j] = replication[j], replication[i]
		})

		edge = min(limit-lnM, lnR)
	}

	return append(migration, replication[:edge]...), nil
}

func (s *multiSolver) Epoch() uint64 { return s.pl.NetworkState().Epoch }

func (s *multiSolver) SelfAddr() (multiaddr.Multiaddr, error) { return s.as.SelfAddr() }
func (s *multiSolver) ReservationRatio(ctx context.Context, addr Address) (int, error) {
	graph, err := s.pl.Query(ctx, placement.ContainerID(addr.CID))
	if err != nil {
		return 0, errors.Wrap(err, "reservation ratio computation failed on placement query")
	}

	nodes, err := graph.Filter(func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
		return bucket.GetSelection(group.Selectors, addr.ObjectID.Bytes())
	}).NodeList()
	if err != nil {
		return 0, errors.Wrap(err, "reservation ratio computation failed on graph node list")
	}

	return len(nodes), nil
}

func (s *multiSolver) SelectRemoteStorages(ctx context.Context, addr Address, excl ...multiaddr.Multiaddr) ([]ObjectLocation, error) {
	selfAddr, err := s.as.SelfAddr()
	if err != nil {
		return nil, errors.Wrap(err, "select remote storage nodes failed on get self address")
	}

	nodes, err := s.selectNodes(ctx, addr, excl...)
	if err != nil {
		return nil, errors.Wrap(err, "select remote storage nodes failed on get node list")
	}

	var (
		metSelf   bool
		selfIndex = -1
		res       = make([]ObjectLocation, 0, len(nodes))
	)

	for i := range nodes {
		if nodes[i].Equal(selfAddr) {
			metSelf = true
			selfIndex = i
		}

		res = append(res, ObjectLocation{
			Node:          nodes[i],
			WeightGreater: !metSelf,
		})
	}

	if selfIndex != -1 {
		res = append(res[:selfIndex], res[selfIndex+1:]...)
	}

	return res, nil
}

func (s *multiSolver) selectNodes(ctx context.Context, addr Address, excl ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	graph, err := s.pl.Query(ctx, placement.ContainerID(addr.CID))
	if err != nil {
		return nil, errors.Wrap(err, "select remote storage nodes failed on placement query")
	}

	filter := func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket { return bucket }
	if !addr.ObjectID.Empty() {
		filter = func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
			return bucket.GetSelection(group.Selectors, addr.ObjectID.Bytes())
		}
	}

	return graph.Exclude(excl).Filter(filter).NodeList()
}

func (s *multiSolver) Actual(ctx context.Context, cid CID) bool {
	graph, err := s.pl.Query(ctx, placement.ContainerID(cid))
	if err != nil {
		return false
	}

	nodes, err := graph.NodeList()
	if err != nil {
		return false
	}

	selfAddr, err := s.as.SelfAddr()
	if err != nil {
		return false
	}

	for i := range nodes {
		if nodes[i].Equal(selfAddr) {
			return true
		}
	}

	return false
}

func (s *multiSolver) CompareWeight(ctx context.Context, addr Address, node multiaddr.Multiaddr) int {
	selfAddr, err := s.as.SelfAddr()
	if err != nil {
		return -1
	}

	if selfAddr.Equal(node) {
		return 0
	}

	excl := make([]multiaddr.Multiaddr, 0)

	for {
		nodes, err := s.selectNodes(ctx, addr, excl...)
		if err != nil {
			return -1
		}

		for j := range nodes {
			if nodes[j].Equal(selfAddr) {
				return -1
			} else if nodes[j].Equal(node) {
				return 1
			}
		}

		excl = append(excl, nodes[0]) // TODO: when it will become relevant to append full nodes slice
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
