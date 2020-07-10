package object

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-node/lib/implementations"

	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

type (
	containerTraverser interface {
		implementations.Traverser
		add(multiaddr.Multiaddr, bool)
		done(multiaddr.Multiaddr) bool
		finished() bool
		close()
		Err() error
	}

	placementBuilder interface {
		buildPlacement(context.Context, Address, ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error)
	}

	traverseParams struct {
		tryPrevNM            bool
		addr                 Address
		curPlacementBuilder  placementBuilder
		prevPlacementBuilder placementBuilder
		maxRecycleCount      int
		stopCount            int
	}

	coreTraverser struct {
		closed bool

		usePrevNM bool

		recycleNum int

		*sync.RWMutex
		traverseParams
		failed []multiaddr.Multiaddr
		mDone  map[string]struct{}
		err    error
	}
)

var (
	_ placementBuilder   = (*corePlacementUtil)(nil)
	_ containerTraverser = (*coreTraverser)(nil)
)

func (s *coreTraverser) Next(ctx context.Context) []multiaddr.Multiaddr {
	if s.isClosed() || s.finished() {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	return s.next(ctx)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func (s *coreTraverser) next(ctx context.Context) (nodes []multiaddr.Multiaddr) {
	defer func() {
		if s.stopCount == 0 {
			s.stopCount = len(nodes)
		}

		if s.stopCount > 0 {
			nodes = nodes[:minInt(
				s.stopCount-len(s.mDone),
				len(nodes),
			)]
		}
	}()

	var placeBuilder = s.curPlacementBuilder
	if s.usePrevNM {
		placeBuilder = s.prevPlacementBuilder
	}

	nodes, s.err = placeBuilder.buildPlacement(ctx, s.addr, s.failed...)
	if errors.Is(errors.Cause(s.err), container.ErrNotFound) {
		return
	}

	for i := 0; i < len(nodes); i++ {
		if _, ok := s.mDone[nodes[i].String()]; ok {
			nodes = append(nodes[:i], nodes[i+1:]...)
			i--
		}

		continue
	}

	if len(nodes) == 0 {
		if !s.usePrevNM && s.tryPrevNM {
			s.usePrevNM = true
			return s.next(ctx)
		}

		if s.recycleNum < s.maxRecycleCount {
			s.reset()
			return s.next(ctx)
		}
	}

	return nodes
}

func (s *coreTraverser) reset() {
	s.usePrevNM = false
	s.failed = s.failed[:0]
	s.recycleNum++
}

func (s *coreTraverser) add(node multiaddr.Multiaddr, ok bool) {
	s.Lock()
	if ok {
		s.mDone[node.String()] = struct{}{}
	} else {
		s.failed = append(s.failed, node)
	}
	s.Unlock()
}

func (s *coreTraverser) done(node multiaddr.Multiaddr) bool {
	s.RLock()
	_, ok := s.mDone[node.String()]
	s.RUnlock()

	return ok
}

func (s *coreTraverser) close() {
	s.Lock()
	s.closed = true
	s.Unlock()
}

func (s *coreTraverser) isClosed() bool {
	s.RLock()
	defer s.RUnlock()

	return s.closed
}

func (s *coreTraverser) finished() bool {
	s.RLock()
	defer s.RUnlock()

	return s.stopCount > 0 && len(s.mDone) >= s.stopCount
}

func (s *coreTraverser) Err() error {
	s.RLock()
	defer s.RUnlock()

	return s.err
}

func newContainerTraverser(p *traverseParams) containerTraverser {
	return &coreTraverser{
		RWMutex:        new(sync.RWMutex),
		traverseParams: *p,
		failed:         make([]multiaddr.Multiaddr, 0),
		mDone:          make(map[string]struct{}),
	}
}

func (s *corePlacementUtil) buildPlacement(ctx context.Context, addr Address, excl ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	return s.placementBuilder.GetNodes(ctx, addr, s.prevNetMap, excl...)
}
