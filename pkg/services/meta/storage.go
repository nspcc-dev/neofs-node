package meta

import (
	"context"
	"time"

	metabase "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// PutObjects forces [Meta] to index provided objects.
func (m *Meta) PutObjects(o []*object.Object) error {
	return m.metabase.PutBatch(o)
}

func (m *Meta) storager(ctx context.Context, buff <-chan storageTask) {
	ch := make(chan *object.Object, cap(buff))
	go batchWriter(ctx, m.l, m.metabase, ch)

	for {
		if len(buff) >= notificationBuffSize-1 {
			m.l.Warn("storage task queue buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case n := <-buff:
			cID := n.addr.Container()
			ok, err := m.net.IsMineWithMeta(cID, nil)
			if err != nil {
				m.l.Error("failed to check container relation to node", zap.Stringer("addr", n.addr), zap.Error(err))
				continue
			}
			if !ok {
				continue
			}

			if n.o == nil {
				o, err := m.net.Head(ctx, n.addr.Container(), n.addr.Object())
				if err != nil {
					m.l.Error("cannot fetch object header", zap.Stringer("addr", n.addr), zap.Error(err))
					continue
				}

				n.o = &o
			}

			ch <- n.o
		}
	}
}

func batchWriter(ctx context.Context, l *zap.Logger, m *metabase.DB, buff <-chan *object.Object) {
	const (
		maxBuffSize   = 100
		maxBatchDelay = time.Second
	)
	var (
		t          = time.NewTicker(maxBatchDelay)
		batch      = make([]*object.Object, 0, maxBuffSize)
		writeBatch = func() {
			err := m.PutBatch(batch)
			if err != nil {
				l.Error("failed to put objects batch", zap.Int("batchSize", len(batch)), zap.Error(err))
			}
			batch = batch[:0]
			t.Reset(maxBatchDelay)
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
				t.Reset(maxBatchDelay)
			}
			batch = append(batch, o)
			if len(batch) == maxBuffSize {
				writeBatch()
			}
		}
	}
}
