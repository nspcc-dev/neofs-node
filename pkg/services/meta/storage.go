package meta

import (
	"context"
	"sync"
	"time"

	metabase "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// storagerConcurrency defines how many storage tasks have their headers
// resolved simultaneously, matching the bound split.Verifier uses for the same
// kind of per-object network HEAD.
const storagerConcurrency = 16

// PutObjects forces [Meta] to index provided objects.
func (m *Meta) PutObjects(o []*object.Object) error {
	return m.metabase.PutBatch(o)
}

func (m *Meta) storager(ctx context.Context, buff <-chan storageTask) {
	ch := make(chan *object.Object, cap(buff))
	go batchWriter(ctx, m.l, m.batchFlushInterval, m.metabase, ch)

	tasks := make([]storageTask, 0, storagerConcurrency)

	for {
		if len(buff) >= notificationBuffSize-1 {
			m.l.Warn("storage task queue buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case n := <-buff:
			tasks = append(tasks, n)
		}

		// Whatever else is already queued joins this group, so a slow peer
		// delays one group of headers instead of every task behind it in turn.
	drain:
		for len(tasks) < storagerConcurrency {
			select {
			case n := <-buff:
				tasks = append(tasks, n)
			default:
				break drain
			}
		}

		for _, o := range m.resolveTasks(ctx, tasks) {
			if o == nil {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- o:
			}
		}

		tasks = tasks[:0]
	}
}

// resolveTasks resolves every task's header concurrently and returns them in
// the order the tasks were given. The order is load-bearing:
// [metabase.DB.PutBatch] applies a batch in slice order within a single
// transaction, so for a lock and a tombstone of the same object it decides
// which one is indexed and which is skipped as non-critical. Tasks reach here
// in metadata chain order, which is the same on every node holding the
// container, and resolving them out of order would make that outcome depend on
// local header-fetch timing instead.
//
// A task whose header cannot be resolved gets a nil entry and is left out,
// without disturbing the order of the rest.
func (m *Meta) resolveTasks(ctx context.Context, tasks []storageTask) []*object.Object {
	res := make([]*object.Object, len(tasks))

	var wg sync.WaitGroup
	for i, n := range tasks {
		wg.Go(func() {
			cID := n.addr.Container()

			ok, err := m.net.IsMineWithMeta(cID, nil)
			if err != nil {
				m.l.Error("failed to check container relation to node", zap.Stringer("addr", n.addr), zap.Error(err))
				return
			}
			if !ok {
				return
			}

			if n.o == nil {
				o, err := m.net.Head(ctx, cID, n.addr.Object())
				if err != nil {
					m.l.Error("cannot fetch object header", zap.Stringer("addr", n.addr), zap.Error(err))
					return
				}

				n.o = &o
			}

			res[i] = n.o
		})
	}
	wg.Wait()

	return res
}

func batchWriter(ctx context.Context, l *zap.Logger, batchFlushThreshold time.Duration, m *metabase.DB, buff <-chan *object.Object) {
	const (
		maxBuffSize = 100
	)
	var (
		t          = time.NewTicker(batchFlushThreshold)
		batch      = make([]*object.Object, 0, maxBuffSize)
		writeBatch = func() {
			err := m.PutBatch(batch)
			if err != nil {
				l.Error("failed to put objects batch", zap.Int("batchSize", len(batch)), zap.Error(err))
			}
			batch = batch[:0]
			t.Reset(batchFlushThreshold)
		}
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if len(batch) == 0 {
				t.Stop()
				continue
			}
			writeBatch()
		case o := <-buff:
			if len(batch) == 0 {
				t.Reset(batchFlushThreshold)
			}
			batch = append(batch, o)
			if len(batch) == maxBuffSize {
				writeBatch()
			}
		}
	}
}
