package policer

import (
	"context"
	"errors"
	"fmt"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

func (p *Policer) Run(ctx context.Context) {
	defer func() {
		p.log.Info("routine stopped")
	}()

	go p.poolCapacityWorker(ctx)
	p.shardPolicyWorker(ctx)
}

func (p *Policer) shardPolicyWorker(ctx context.Context) {
	p.mtx.RLock()
	repCooldown := p.repCooldown
	batchSize := p.batchSize
	p.mtx.RUnlock()

	var (
		addrs  []objectcore.AddressWithAttributes
		cursor *engine.Cursor
		err    error
	)

	t := time.NewTimer(repCooldown)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		addrs, cursor, err = p.localStorage.ListWithCursor(batchSize, cursor, iec.AttributeRuleIdx, iec.AttributePartIdx, object.FilterParentID)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				time.Sleep(repCooldown) // finished whole cycle, sleep a bit
				p.log.Info("finished local storage cycle")
				continue
			}
			err = fmt.Errorf("cannot list objects in engine: %w", err)
			p.log.Warn("failure at object select for replication", zap.Error(err))
		}

		for i := range addrs {
			select {
			case <-ctx.Done():
				return
			default:
				addr := addrs[i]
				if p.objsInWork.inWork(addr.Address) {
					// do not process an object
					// that is in work
					continue
				}

				err = p.taskPool.Submit(func() {
					p.objsInWork.add(addr.Address)

					p.processObject(ctx, addr)

					p.objsInWork.remove(addr.Address)
				})
				if err != nil {
					p.log.Warn("pool submission", zap.Error(err))
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.mtx.RLock()
			t.Reset(p.repCooldown)
			p.mtx.RUnlock()
		}
	}
}

func (p *Policer) poolCapacityWorker(ctx context.Context) {
	p.mtx.RLock()
	maxCapacity := p.maxCapacity
	p.mtx.RUnlock()

	ticker := time.NewTicker(p.rebalanceFreq)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			neofsSysLoad := p.loader.ObjectServiceLoad()
			newCapacity := int((1.0 - neofsSysLoad) * float64(maxCapacity))
			if newCapacity == 0 {
				newCapacity++
			}

			if p.taskPool.Cap() != newCapacity {
				p.taskPool.Tune(newCapacity)
				p.log.Debug("tune replication capacity",
					zap.Float64("system_load", neofsSysLoad),
					zap.Int("new_capacity", newCapacity))
			}
		}
	}
}
