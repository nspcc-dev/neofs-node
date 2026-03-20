package policer

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (p *Policer) Run(ctx context.Context) {
	defer func() {
		p.checkECPartsWorkerPool.Release()
		p.log.Info("routine stopped")
	}()

	p.metrics.SetPolicerConsistency(false)
	p.hadToReplicate.Store(false)

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

func randomAddress() oid.Address {
	var cnr cid.ID
	var obj oid.ID
	_, _ = rand.Read(cnr[:])
	_, _ = rand.Read(obj[:])
	var addr oid.Address
	addr.SetContainer(cnr)
	addr.SetObject(obj)
	return addr
}

func (p *Policer) shardPolicyWorker(ctx context.Context) {
	var (
		addrs    []objectcore.AddressWithAttributes
		cursor   *engine.Cursor
		win      boostWindow
		boosted  bool
		err      error
		wrapped  bool
		stopAddr oid.Address
	)

	p.mtx.RLock()
	t := time.NewTimer(p.repCooldown)
	p.mtx.RUnlock()

	stopAddr = randomAddress()
	cursor = engine.NewCursor(stopAddr.Container(), stopAddr.Object())

	cycleFinished := func() {
		cleanCycle := !p.hadToReplicate.Swap(false)
		if cleanCycle {
			p.metrics.SetPolicerConsistency(true)
		}

		p.log.Info("finished local storage cycle", zap.Bool("cleanCycle", cleanCycle))

		wrapped = false
	}

	for {
		var hadReplicationBeforeReset bool

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
				if wrapped {
					cycleFinished()
					time.Sleep(repCooldown)
				} else {
					wrapped = true
					cursor = nil
				}
			} else {
				p.log.Warn("failure at object select for replication", zap.Error(err))
				time.Sleep(repCooldown)
			}
			continue
		}

		for i := range addrs {
			if wrapped && addrs[i].Address.Compare(stopAddr) > 0 {
				hadReplicationBeforeReset = p.hadToReplicate.Load()
				cycleFinished()
			}

			select {
			case <-ctx.Done():
				return
			default:
				addr := addrs[i]
				if !p.objsInWork.tryAdd(addr.Address) {
					// do not process an object
					// that is in work
					continue
				}

				err = p.taskPool.Submit(func() {
					defer p.objsInWork.remove(addr.Address)

					p.processObject(ctx, addr)
				})
				if err != nil {
					p.objsInWork.remove(addr.Address)
					p.log.Warn("pool submission", zap.Error(err))
				}
			}
		}

		// After each batch, record whether replication was needed and update the
		// sliding window. Boost mode transitions only when a strict majority
		// of the window agrees, so an equal split keeps the current state unchanged.
		if boostMultiplier > 1 {
			hadReplication := hadReplicationBeforeReset || p.hadToReplicate.Load()
			withReplication := win.record(hadReplication)

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
