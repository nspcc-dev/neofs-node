package policer

import (
	"context"
	"errors"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (p *Policer) Run(ctx context.Context) {
	defer func() {
		p.log.Info("routine stopped")
	}()

	go p.poolCapacityWorker(ctx)
	p.shardPolicyWorker(ctx)
}

func (p *Policer) shardPolicyWorker(ctx context.Context) {
	var (
		addrs  []oid.Address
		cursor *engine.Cursor
		err    error
	)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		addrs, cursor, err = p.jobQueue.Select(cursor, p.batchSize)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				time.Sleep(time.Second) // finished whole cycle, sleep a bit
				continue
			}
			p.log.Warn("failure at object select for replication", logger.FieldError(err))
		}

		for i := range addrs {
			select {
			case <-ctx.Done():
				return
			default:
				addr := addrs[i]
				if p.objsInWork.inWork(addr) {
					// do not process an object
					// that is in work
					continue
				}

				err = p.taskPool.Submit(func() {
					v, ok := p.cache.Get(addr)
					if ok && time.Since(v.(time.Time)) < p.evictDuration {
						return
					}

					p.objsInWork.add(addr)

					p.processObject(ctx, addr)

					p.cache.Add(addr, time.Now())
					p.objsInWork.remove(addr)
				})
				if err != nil {
					p.log.Warn("pool submission", logger.FieldError(err))
				}
			}
		}
	}
}

func (p *Policer) poolCapacityWorker(ctx context.Context) {
	ticker := time.NewTicker(p.rebalanceFreq)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			neofsSysLoad := p.loader.ObjectServiceLoad()
			newCapacity := int((1.0 - neofsSysLoad) * float64(p.maxCapacity))
			if newCapacity == 0 {
				newCapacity++
			}

			if p.taskPool.Cap() != newCapacity {
				p.taskPool.Tune(newCapacity)
				p.log.Debug("tune replication capacity",
					logger.FieldFloat("system_load", neofsSysLoad),
					logger.FieldInt("new_capacity", int64(newCapacity)),
				)
			}
		}
	}
}
