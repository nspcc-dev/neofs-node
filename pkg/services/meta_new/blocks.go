package meta

import (
	"context"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var hash = state.CreateNativeContractHash("MetaData")

func (m *Meta) handleBlock(b *block.Header) error {
	h := b.Hash()
	ind := b.Index
	l := m.l.With(zap.Stringer("block hash", h), zap.Uint32("index", ind))
	l.Debug("handling block")

	res, err := m.ws.GetBlockNotifications(h, &neorpc.NotificationFilter{
		Contract: &hash,
	})
	if err != nil {
		return fmt.Errorf("fetching %s block: %w", h, err)
	}

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
