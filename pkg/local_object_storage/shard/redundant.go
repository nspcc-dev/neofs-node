package shard

import (
	"errors"
	"slices"
	"sync"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const redundantQueueLimit = 10_000

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
	count   int
	pending map[cid.ID][]oid.ID
}

func (q *redundantQueue) add(cnr cid.ID, ids []oid.ID) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return errRedundantQueueClosed
	}
	n := min(len(ids), redundantQueueLimit-q.count)
	if n > 0 {
		q.pending[cnr] = append(q.pending[cnr], ids[:n]...)
		q.count += n
	}
	if n < len(ids) {
		return errRedundantQueueFull
	}
	return nil
}

// remove forgets completed or canceled tasks.
func (q *redundantQueue) remove(cnr cid.ID, ids []oid.ID) {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := q.pending[cnr]
	remaining := slices.DeleteFunc(pending, func(id oid.ID) bool {
		return slices.Contains(ids, id)
	})
	q.count -= len(pending) - len(remaining)
	if len(remaining) == 0 {
		delete(q.pending, cnr)
	} else {
		q.pending[cnr] = remaining
	}
}

func (q *redundantQueue) setClosed(closed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = closed
}

// batch returns a snapshot without removing tasks. Failed tasks remain pending.
func (q *redundantQueue) batch(limit int) []meta.TrashBin {
	if limit <= 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var bins []meta.TrashBin
	for cnr, ids := range q.pending {
		n := min(limit, len(ids))
		bins = append(bins, meta.TrashBin{Container: cnr, Objects: slices.Clone(ids[:n])})
		limit -= n
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
