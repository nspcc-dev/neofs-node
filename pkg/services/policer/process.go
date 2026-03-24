package policer

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
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
	p.metrics.SetPolicerOptimalPlacement(false)
	p.hadReplicaShortage.Store(false)
	p.hadPlacementMismatch.Store(false)

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
		wg       sync.WaitGroup
		curTick  time.Duration
	)

	p.mtx.RLock()
	curTick = p.repCooldown
	t := time.NewTicker(curTick)
	p.mtx.RUnlock()

	stopAddr = randomAddress()
	cursor = engine.NewCursor(stopAddr.Container(), stopAddr.Object())

	cycleFinished := func() {
		cleanShortageCycle := !p.hadReplicaShortage.Swap(false)
		cleanPlacementCycle := !p.hadPlacementMismatch.Swap(false)
		if cleanShortageCycle {
			p.metrics.SetPolicerConsistency(true)
		}
		if cleanPlacementCycle {
			p.metrics.SetPolicerOptimalPlacement(true)
		}

		p.log.Info("finished local storage cycle",
			zap.Bool("cleanShortageCycle", cleanShortageCycle),
			zap.Bool("cleanPlacementCycle", cleanPlacementCycle))

		wrapped = false
	}

	for {
		var hadShortageBeforeReset bool

		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		p.mtx.RLock()
		repCooldown := p.repCooldown
		baseBatchSize := p.batchSize
		boostMultiplier := p.boostMultiplier
		p.mtx.RUnlock()

		if curTick != repCooldown {
			curTick = repCooldown
			t.Reset(curTick)
		}

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
				} else {
					wrapped = true
					cursor = nil
				}
			} else {
				p.log.Warn("failure at object select for replication", zap.Error(err))
			}
			continue
		}

		for _, addr := range addrs {
			if wrapped && addr.Address.Compare(stopAddr) > 0 {
				hadShortageBeforeReset = p.hadReplicaShortage.Load()
				cycleFinished()
			}
			wg.Go(func() {
				p.processObject(ctx, addr)
			})
		}

		wg.Wait()
		// After each batch, record whether replication was needed and update the
		// sliding window. Boost mode transitions only when a strict majority
		// of the window agrees, so an equal split keeps the current state unchanged.
		if boostMultiplier > 1 {
			hadShortage := hadShortageBeforeReset || p.hadReplicaShortage.Load()
			withReplication := win.record(hadShortage)

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
	}
}
