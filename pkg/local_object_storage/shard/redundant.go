package shard

import (
	"errors"
	"sync"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const redundantQueueLimit = 100_000

var (
	errRedundantQueueFull   = logicerr.New("redundant object queue is full")
	errRedundantQueueClosed = logicerr.New("redundant object queue is closed")
)

// redundantQueue retains addresses until they are deleted or persisted.
// There must be only one consumer (GC, or shutdown after GC has stopped).
// Processing order is unspecified.
type redundantQueue struct {
	mu      sync.Mutex
	closed  bool
	pending map[oid.Address]struct{}
}

func (q *redundantQueue) add(cnr cid.ID, ids []oid.ID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return errRedundantQueueClosed
	}
	for _, id := range ids {
		addr := oid.NewAddress(cnr, id)
		if _, ok := q.pending[addr]; ok {
			continue
		}
		if len(q.pending) >= redundantQueueLimit {
			return errRedundantQueueFull
		}
		if q.pending == nil {
			q.pending = make(map[oid.Address]struct{})
		}
		q.pending[addr] = struct{}{}
	}
	return nil
}

// remove forgets completed or canceled tasks.
func (q *redundantQueue) remove(cnr cid.ID, ids []oid.ID) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, id := range ids {
		delete(q.pending, oid.NewAddress(cnr, id))
	}
}

func (q *redundantQueue) setClosed(closed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = closed
}

// batch returns a snapshot without removing tasks. Failed tasks remain pending.
func (q *redundantQueue) batch(limit int) []meta.TrashBin {
	q.mu.Lock()
	defer q.mu.Unlock()

	if limit <= 0 {
		return nil
	}

	var bins []meta.TrashBin
	byContainer := make(map[cid.ID]int)
	for addr := range q.pending {
		cnr := addr.Container()
		i, ok := byContainer[cnr]
		if !ok {
			i = len(bins)
			byContainer[cnr] = i
			bins = append(bins, meta.TrashBin{Container: cnr})
		}
		bins[i].Objects = append(bins[i].Objects, addr.Object())
		limit--
		if limit == 0 {
			break
		}
	}
	return bins
}

// flushRedundant persists unfinished work using the existing on-disk format.
// The caller must hold s.m and stop GC before calling this during shutdown.
func (s *Shard) flushRedundant() error {
	var errs []error
	for _, bin := range s.redundant.batch(redundantQueueLimit) {
		if err := s.markGarbage(bin.Container, bin.Objects, meta.GarbageMarkRedundant); err != nil {
			errs = append(errs, err)
		} else {
			s.redundant.remove(bin.Container, bin.Objects)
		}
	}
	return errors.Join(errs...)
}
