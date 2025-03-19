package meta

import (
	"context"
	"fmt"

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

	evName := objPutEvName
	m.cliM.RLock()
	res, err := m.ws.GetBlockNotifications(h, &neorpc.NotificationFilter{
		Contract: &m.cnrH,
		Name:     &evName,
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

	m.stM.RLock()
	defer m.stM.RUnlock()

	evsByStorage := make(map[*containerStorage][]objEvent)
	for _, n := range res.Application {
		ev, err := parseObjNotification(n)
		if err != nil {
			l.Error("invalid object notification received", zap.Error(err))
			continue
		}

		if magic := uint32(ev.network.Uint64()); magic != m.magicNumber {
			l.Warn("skipping object notification with wrong magic number", zap.Uint32("expected", m.magicNumber), zap.Uint32("got", magic))
		}

		st, ok := m.storages[ev.cID]
		if !ok {
			l.Debug("skipping object notification", zap.Stringer("inactual container", ev.cID))
			continue
		}

		m.l.Debug("received object notification", zap.Stringer("address", oid.NewAddress(ev.cID, ev.oID)))

		evsByStorage[st] = append(evsByStorage[st], ev)
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

	return nil
}

func (m *Meta) blockFetcher(ctx context.Context, buff <-chan *block.Header) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-buff:
			err := m.handleBlock(ctx, b)
			if err != nil {
				m.l.Error("block handling failed", zap.Error(err))
				continue
			}
		}
	}
}
