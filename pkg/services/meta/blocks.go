package meta

import (
	"context"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (m *Meta) handleBlock(b *block.Header) error {
	h := b.Hash()
	ind := b.Index
	l := m.l.With(zap.Stringer("block hash", h), zap.Uint32("index", ind))
	l.Debug("handling block")

	m.cliM.RLock()
	res, err := m.ws.GetBlockNotifications(h, &neorpc.NotificationFilter{
		Contract: &m.cnrH,
	})
	if err != nil {
		m.cliM.RUnlock()
		return fmt.Errorf("fetching %s block: %w", h, err)
	}
	m.cliM.RUnlock()

	if len(res.Application) == 0 {
		l.Debug("no notifications for block")
		return nil
	}

	evsByCID := make(map[cid.ID]blockObjEvents)
	for _, n := range res.Application {
		l := m.l.With(zap.Stringer("tx", n.Container))

		switch n.Name {
		case objPutEvName:
			ev, err := parseObjNotification(n)
			if err != nil {
				l.Error("invalid object notification received", zap.Error(err))
				continue
			}

			if magic := uint32(ev.network.Uint64()); magic != m.magicNumber {
				l.Warn("skipping object notification with wrong magic number", zap.Uint32("expected", m.magicNumber), zap.Uint32("got", magic))
				continue
			}

			m.notifier.notifyReceived(oid.NewAddress(ev.cID, ev.oID))

			m.stM.RLock()
			_, ok := m.storages[ev.cID]
			m.stM.RUnlock()
			if !ok {
				l.Debug("skipping object notification", zap.Stringer("container", ev.cID))
				continue
			}

			m.l.Debug("received object notification", zap.Stringer("address", oid.NewAddress(ev.cID, ev.oID)))

			blockEvents, ok := evsByCID[ev.cID]
			if !ok {
				blockEvents = blockObjEvents{cID: ev.cID, bInd: ind}
			}
			blockEvents.evs = append(blockEvents.evs, ev)
			evsByCID[ev.cID] = blockEvents
		case cnrDeleteName, cnrRmName:
			ev, err := parseCnrNotification(n)
			if err != nil {
				l.Error("invalid container removal notification received", zap.Error(err))
				continue
			}

			m.stM.RLock()
			_, ok := m.storages[ev.cID]
			m.stM.RUnlock()
			if !ok {
				l.Debug("skipping container notification", zap.Stringer("container", ev.cID))
				continue
			}

			err = m.dropContainer(ev.cID)
			if err != nil {
				l.Error("deleting container failed", zap.Error(err))
				continue
			}

			l.Debug("deleted container", zap.Stringer("cID", ev.cID))
		case cnrPutName, cnrCrtName:
			ev, err := parseCnrNotification(n)
			if err != nil {
				l.Error("invalid container notification received", zap.Error(err))
				continue
			}

			go m.addContainerIfMine(l, ev.cID)
		default:
			l.Debug("skip notification", zap.String("event name", n.Name))
			continue
		}
	}

	if len(evsByCID) == 0 {
		return nil
	}

	for _, ev := range evsByCID {
		m.blockEventsBuff <- ev
	}

	l.Debug("handled block successfully", zap.Int("num of notifications", len(res.Application)))

	return nil
}

type blockObjEvents struct {
	cID  cid.ID
	bInd uint32
	evs  []objEvent
}

func (m *Meta) blockStorer(ctx context.Context, buff <-chan blockObjEvents, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if len(buff) == blockBuffSize {
			m.l.Warn("block notifications buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case blockEvs := <-buff:
			m.stM.RLock()
			st, ok := m.storages[blockEvs.cID]
			m.stM.RUnlock()
			if !ok {
				m.l.Debug("do not store inactual events", zap.Stringer("cID", blockEvs.cID), zap.Uint32("events from block", blockEvs.bInd))
				continue
			}

			st.putObjects(ctx, m.l.With(zap.String("storage", st.path)), blockEvs.bInd, blockEvs.evs, m.net)

			m.l.Debug("stored container's notification for block successfully",
				zap.Int("num of notifications", len(blockEvs.evs)),
				zap.Stringer("cID", blockEvs.cID),
				zap.Uint32("events from block", blockEvs.bInd))
		}
	}
}

func (m *Meta) blockHandler(ctx context.Context, buff <-chan *block.Header, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if len(buff) == blockBuffSize {
			m.l.Warn("block header buffer has been completely filled")
		}

		select {
		case <-ctx.Done():
			return
		case b := <-buff:
			err := m.handleBlock(b)
			if err != nil {
				m.l.Error("block handling failed", zap.Error(err))
				continue
			}
		}
	}
}
