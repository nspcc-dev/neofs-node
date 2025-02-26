package meta

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
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
		return nil
	}

	m.m.RLock()
	defer m.m.RUnlock()

	var wg errgroup.Group
	wg.SetLimit(1024)
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	for _, n := range res.Application {
		ev, err := parseObjNotification(n)
		if err != nil {
			l.Error("invalid object notification received", zap.Error(err))
			continue
		}

		s, ok := m.storages[ev.cID]
		if !ok {
			l.Debug("skipping object notification", zap.Stringer("inactual container", ev.cID))
			continue
		}

		wg.Go(func() error {
			err := m.handleObjectNotification(ctx, s, ev)
			if err != nil {
				return fmt.Errorf("handling %s/%s object notification: %w", ev.cID, ev.oID, err)
			}

			l.Debug("handled object notification successfully", zap.Stringer("cID", ev.cID), zap.Stringer("oID", ev.oID))

			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		l.Error("failed to handle block's notifications", zap.Error(err))
	}

	for _, st := range m.storages {
		// TODO: parallelize depending on what can parallelize well

		st.m.Lock()

		root := st.mpt.StateRoot()
		st.mpt.Store.Put([]byte{rootKey}, root[:])
		p := st.path
		if st.opsBatch != nil {
			_, err := st.mpt.PutBatch(mpt.MapToMPTBatch(st.opsBatch))
			if err != nil {
				st.m.Unlock()
				return fmt.Errorf("put batch for %d block to %q storage: %w", ind, p, err)
			}

			st.opsBatch = nil
		}

		st.m.Unlock()

		st.mpt.Flush(ind)
	}

	l.Debug("handled block successfully")

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
