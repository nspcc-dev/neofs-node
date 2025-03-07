package policer

import (
	"context"
	"errors"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
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
	p.cfg.RLock()
	repCooldown := p.repCooldown
	batchSize := p.batchSize
	p.cfg.RUnlock()

	var (
		addrs  []objectcore.AddressWithType
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

		addrs, cursor, err = p.jobQueue.Select(cursor, batchSize)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				time.Sleep(time.Second) // finished whole cycle, sleep a bit
				continue
			}
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
			p.cfg.RLock()
			t.Reset(p.repCooldown)
			p.cfg.RUnlock()
		}
	}
}

func (p *Policer) poolCapacityWorker(ctx context.Context) {
	p.cfg.RLock()
	maxCapacity := p.maxCapacity
	p.cfg.RUnlock()

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
