package policer

import (
	"context"
	"errors"
	"time"

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
	var (
		addrs  []*object.Address
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
			p.log.Warn("failure at object select for replication", zap.Error(err))
		}

		for i := range addrs {
			select {
			case <-ctx.Done():
				return
			default:
				addr := addrs[i]
				addrStr := addr.String()
				err = p.taskPool.Submit(func() {
					v, ok := p.cache.Get(addrStr)
					if ok && time.Since(v.(time.Time)) < p.evictDuration {
						return
					}

					p.processObject(ctx, addr)
					p.cache.Add(addrStr, time.Now())
				})
				if err != nil {
					p.log.Warn("pool submission", zap.Error(err))
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
					zap.Float64("system_load", neofsSysLoad),
					zap.Int("new_capacity", newCapacity))
			}
		}
	}
}
