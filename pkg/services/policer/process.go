package policer

import (
	"context"
	"errors"
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

	p.metrics.SetPolicerConsistency(false)
	p.hadToReplicate.Store(false)

	go p.poolCapacityWorker(ctx)
	p.shardPolicyWorker(ctx)
}

const (
	// boostWindowSize is the number of recent batches tracked to compute the
	// current boost multiplier.
	boostWindowSize = 8
	// boostMajority is the minimum number of batches in the sliding window that
	// must require replication to enter boost mode (or be clean to leave it).
	// An equal split keeps the current state unchanged.
	boostMajority = boostWindowSize/2 + 1
)

// boostWindow is a fixed-size circular buffer that records whether each of the
// last [boostWindowSize] batches required any replication or was clean.
// It is used to decide when to enter or leave boost mode.
type boostWindow struct {
	buf [boostWindowSize]bool
	pos int
}

// record adds a new observation to the window and returns the number of
// batches that required replication among the ones currently in the window.
func (w *boostWindow) record(hadReplicated bool) (withReplication int) {
	w.buf[w.pos] = hadReplicated
	w.pos = (w.pos + 1) % boostWindowSize
	for _, v := range w.buf {
		if v {
			withReplication++
		}
	}
	return withReplication
}

func (p *Policer) shardPolicyWorker(ctx context.Context) {
	var (
		addrs   []objectcore.AddressWithAttributes
		cursor  *engine.Cursor
		win     boostWindow
		boosted bool
		err     error
	)

	p.mtx.RLock()
	t := time.NewTimer(p.repCooldown)
	p.mtx.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.mtx.RLock()
		repCooldown := p.repCooldown
		baseBatchSize := p.batchSize
		boostMultiplier := p.boostMultiplier
		p.mtx.RUnlock()

		boostMultiplier = max(boostMultiplier, 1)

		batchSize := baseBatchSize
		if boosted {
			batchSize *= boostMultiplier
		}

		addrs, cursor, err = p.localStorage.ListWithCursor(batchSize, cursor, iec.AttributeRuleIdx, iec.AttributePartIdx, object.FilterParentID)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				cleanCycle := !p.hadToReplicate.Swap(false)
				if cleanCycle {
					p.metrics.SetPolicerConsistency(true)
				}

				p.log.Info("finished local storage cycle", zap.Bool("cleanCycle", cleanCycle))
			} else {
				p.log.Warn("failure at object select for replication", zap.Error(err))
			}
			time.Sleep(repCooldown)
			continue
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

		// After each batch, record whether replication was needed and update the
		// sliding window. Boost mode transitions only when a strict majority
		// of the window agrees, so an equal split keeps the current state unchanged.
		if boostMultiplier > 1 {
			withReplication := win.record(p.hadToReplicate.Load())

			if !boosted && withReplication >= boostMajority {
				p.log.Info("missing replicas detected, entering boost mode",
					zap.Uint32("multiplier", boostMultiplier),
					zap.Uint32("batch_size", baseBatchSize),
					zap.Int("batches_with_replication", withReplication))
				boosted = true
			} else if boosted && (boostWindowSize-withReplication) >= boostMajority {
				p.log.Info("recovery complete, leaving boost mode",
					zap.Int("clean_batches", boostWindowSize-withReplication))
				boosted = false
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-t.C:
			t.Reset(repCooldown)
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
