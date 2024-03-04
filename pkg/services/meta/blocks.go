package meta

import (
	"context"
	"fmt"
	"sync"

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

		evsByStorage[st] = append(evsByStorage[st], ev)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for st, ee := range evsByStorage {
			st.putMPTIndexes(ee)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()

		var internalWg errgroup.Group
		internalWg.SetLimit(1024)

		for st, evs := range evsByStorage {
			internalWg.Go(func() error {
				err := st.putRawIndexes(ctx, evs, m.net)
				if err != nil {
					l.Error("failed to put raw indexes", zap.String("storage", st.path), zap.Error(err))
				}

				// do not stop other routines ever
				return nil
			})
		}

		// errors are logged, no errors are returned to WG
		_ = internalWg.Wait()
	}()

	wg.Wait()

	for st := range evsByStorage {
		// TODO: parallelize depending on what can parallelize well

		st.m.Lock()

		root := st.mpt.StateRoot()
		st.mpt.Store.Put([]byte{rootKey}, root[:])
		p := st.path

		_, err := st.mpt.PutBatch(mpt.MapToMPTBatch(st.mptOpsBatch))
		if err != nil {
			st.m.Unlock()
			return fmt.Errorf("put batch for %d block to %q storage: %w", ind, p, err)
		}

		clear(st.mptOpsBatch)

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
