package meta

import (
	"context"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (m *Meta) blockHandler(ctx context.Context, buff <-chan *block.Header, wg *sync.WaitGroup) {
	defer wg.Done()

	prevBlockFetchTime := time.Now()
	for {
		if len(buff) == blockBuffSize {
			m.l.Warn("block header buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case b := <-buff:
			blockReceivedAfter := time.Since(prevBlockFetchTime)
			prevBlockFetchTime = time.Now()

			h := b.Hash()
			ind := b.Index
			m.l.Debug("received block", zap.Stringer("block hash", h), zap.Uint32("index", ind))

			m.chainHeigh.Store(ind)

			m.metrics.newBlockFetchTime.Observe(blockReceivedAfter.Seconds())
		}
	}
}

func (m *Meta) notificationHandler(ctx context.Context, buff <-chan *state.ContainedNotificationEvent, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if len(buff) == notificationBuffSize {
			m.l.Warn("notification buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case n := <-buff:
			l := m.l.With(zap.Stringer("tx", n.Container))

			switch n.Name {
			case objPutEvName:
				ev, err := parseObjNotification(*n)
				if err != nil {
					l.Error("invalid object notification received", zap.Error(err))
					continue
				}

				if magic := uint32(ev.network.Uint64()); magic != m.magicNumber {
					l.Warn("skipping object notification with wrong magic number", zap.Uint32("expected", m.magicNumber), zap.Uint32("got", magic))
					continue
				}

				addr := oid.NewAddress(ev.cID, ev.oID)
				m.notifier.notifyReceived(addr)

				l.Debug("object notification successfully handled", zap.Stringer("address", addr))
			default:
				l.Debug("skip notification", zap.String("event name", n.Name))
				continue
			}
		}
	}
}
