package meta

import (
	"context"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (m *Meta) blockHandler(ctx context.Context, buff <-chan *block.Header) {
	for {
		if len(buff) >= blockBuffSize-1 {
			m.l.Warn("block header buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case b := <-buff:
			h := b.Hash()
			ind := b.Index
			m.l.Debug("received block", zap.Stringer("block hash", h), zap.Uint32("index", ind))
		}
	}
}

func (m *Meta) notificationHandler(ctx context.Context, buff <-chan *state.ContainedNotificationEvent) {
	for {
		if len(buff) >= notificationBuffSize-1 {
			m.l.Warn("notification buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case n := <-buff:
			switch n.Name {
			case objPutEvName:
				ev, err := parseObjNotification(*n)
				if err != nil {
					m.l.Error("invalid object notification received", zap.Stringer("tx", n.Container), zap.Error(err))
					continue
				}

				if ev.network != m.magicNumber {
					m.l.Warn("skipping object notification with wrong magic number",
						zap.Stringer("tx", n.Container), zap.Uint32("expected", m.magicNumber),
						zap.Uint32("got", ev.network))
					continue
				}

				addr := oid.NewAddress(ev.cID, ev.oID)
				m.notifier.notifyReceived(addr)

				m.l.Debug("object notification successfully handled", zap.Stringer("tx", n.Container), zap.Stringer("address", addr))
			default:
				m.l.Debug("skip notification", zap.Stringer("tx", n.Container), zap.String("event name", n.Name))
				continue
			}
		}
	}
}
