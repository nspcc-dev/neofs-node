package meta

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (m *Meta) handleBlock(ctx context.Context, b *block.Header) error {
	h := b.Hash()
	ind := b.Index
	l := m.l.With(zap.Stringer("block hash", h), zap.Uint32("index", ind))
	l.Debug("handling block")

	start := time.Now()

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

	getNotifications := time.Since(start)
	start = time.Now()

	evsByStorage := make(map[*containerStorage][]objEvent)
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

			m.notifier.notifyReceived(oid.NewAddress(ev.cID, ev.oID), ind)

			m.stM.RLock()
			st, ok := m.storages[ev.cID]
			m.stM.RUnlock()
			if !ok {
				l.Debug("skipping object notification", zap.Stringer("container", ev.cID))
				continue
			}

			m.l.Debug("received object notification", zap.Stringer("address", oid.NewAddress(ev.cID, ev.oID)))

			evsByStorage[st] = append(evsByStorage[st], ev)
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

			ok, err := m.net.IsMineWithMeta(ev.cID)
			if err != nil {
				l.Error("can't get container data", zap.Error(err))
				continue
			}
			if !ok {
				l.Debug("skip new inactual container", zap.Stringer("cid", ev.cID))
				continue
			}

			err = m.addContainer(ev.cID)
			if err != nil {
				l.Error("could not handle new container", zap.Stringer("cID", ev.cID), zap.Error(err))
				continue
			}

			l.Debug("added container storage", zap.Stringer("cID", ev.cID))
		default:
			l.Debug("skip notification", zap.String("event name", n.Name))
			continue
		}
	}

	parsingNotifications := time.Since(start)
	start = time.Now()

	if len(evsByStorage) == 0 {
		return nil
	}

	var wg errgroup.Group
	wg.SetLimit(1024)

	for st, evs := range evsByStorage {
		wg.Go(func() error {
			st.putObjects(ctx, l.With(zap.String("storage", st.path)), ind, evs, m.net)
			return nil
		})
	}

	// errors are logged, no errors are returned to WG
	_ = wg.Wait()
	l.Debug("handled block successfully", zap.Int("num of notifications", len(res.Application)))

	storingObjs := time.Since(start)
	m.l.Info("handling notifications", zap.Int64("getNotifications", getNotifications.Milliseconds()), zap.Int64("parsingNotifications", parsingNotifications.Milliseconds()), zap.Int64("storingObjs", storingObjs.Milliseconds()))

	return nil
}

func (m *Meta) blockFetcher(ctx context.Context, buff <-chan *block.Header) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-buff:
			t := time.Now()
			err := m.handleBlock(ctx, b)
			took := time.Since(t)
			m.l.Info("handling block time", zap.Int64("took", took.Milliseconds()))
			if err != nil {
				m.l.Error("block handling failed", zap.Error(err))
				continue
			}
		}
	}
}

func (m *Meta) CurrentBlockHeight() uint32 {
	return m.currHeight.Load()
}
