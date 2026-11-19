package meta

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/bbolt"
	"go.uber.org/zap"
)

// PUT write controller parameters. The transaction delay grows from
// minTxDelay to maxTxDelay with the smoothed incoming write rate.
const (
	minTxDelay      = time.Millisecond
	maxTxDelay      = 20 * time.Millisecond
	minWritesPerSec = 0
	maxWritesPerSec = 400
	txDelayShape    = 4
	writesEMAFactor = 0.3

	// maxPutBatchBytes limits the total header size of objects put in one
	// transaction.
	maxPutBatchBytes = 1 << 20

	writeControllerTick = time.Second
	// putStatsTicks is the number of controller ticks between batch statistics
	// log records.
	putStatsTicks = 10
)

var errPutTrySolo = errors.New("put batch function returned an error and should be re-run solo")

// writeController collects PUT writes in transactions. The delay before a
// transaction starts follows the incoming write rate.
type writeController struct {
	writes  atomic.Uint64
	txDelay atomic.Int64

	emaWritesPerSec float64
	ticks           uint

	stop chan struct{}
	done chan struct{}

	// commitMtx serializes batch transactions, so a pending batch keeps
	// collecting calls while the previous one commits.
	commitMtx sync.Mutex

	mtx     sync.Mutex
	pending *putBatch
	stats   putBatchStats
}

type putBatchStats struct {
	batches  uint64
	objects  uint64
	total    time.Duration
	min, max time.Duration
}

type putCall struct {
	fn  func(*bbolt.Tx) error
	err chan<- error
}

type putBatch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []putCall
	size  int
}

// txDelay returns the delay before a PUT transaction for the given smoothed
// write rate.
func txDelay(emaWritesPerSec float64) time.Duration {
	load := (emaWritesPerSec - minWritesPerSec) / (maxWritesPerSec - minWritesPerSec)
	load = min(max(load, 0), 1)
	d := minTxDelay + time.Duration(float64(maxTxDelay-minTxDelay)*math.Pow(load, txDelayShape))
	return min(max(d, minTxDelay), maxTxDelay)
}

// nextWritesEMA returns the smoothed write rate after a tick with
// writesPerSec writes.
func nextWritesEMA(prev, writesPerSec float64) float64 {
	return writesEMAFactor*writesPerSec + (1-writesEMAFactor)*prev
}

func (db *DB) startWriteController() {
	c := &db.putCtrl
	if c.txDelay.Load() == 0 {
		c.txDelay.Store(int64(minTxDelay))
	}
	c.stop = make(chan struct{})
	c.done = make(chan struct{})
	go db.runWriteController(c.stop, c.done)
}

func (db *DB) stopWriteController() {
	c := &db.putCtrl
	if c.stop == nil {
		return
	}
	close(c.stop)
	<-c.done
	c.stop, c.done = nil, nil
}

func (db *DB) runWriteController(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	t := time.NewTicker(writeControllerTick)
	defer t.Stop()

	for {
		select {
		case <-stop:
			return
		case <-t.C:
			db.tickWriteController()
		}
	}
}

func (db *DB) tickWriteController() {
	c := &db.putCtrl
	writesPerSec := float64(c.writes.Swap(0)) / writeControllerTick.Seconds()
	c.emaWritesPerSec = nextWritesEMA(c.emaWritesPerSec, writesPerSec)
	d := txDelay(c.emaWritesPerSec)
	c.txDelay.Store(int64(d))

	c.ticks++
	if c.ticks%putStatsTicks != 0 {
		return
	}

	c.mtx.Lock()
	s := c.stats
	c.stats = putBatchStats{}
	c.mtx.Unlock()

	if s.batches == 0 {
		return
	}
	db.log.Info("metabase put batch stats",
		zap.Uint64("batches", s.batches),
		zap.Uint64("objects", s.objects),
		zap.Duration("avg", s.total/time.Duration(s.batches)),
		zap.Duration("min", s.min),
		zap.Duration("max", s.max),
		zap.Float64("writes_per_sec", c.emaWritesPerSec),
		zap.Duration("tx_delay", d))
}

// putBatch calls fn as a part of a write transaction shared with concurrent
// putBatch calls. The transaction starts after the controller's current delay,
// or at once when the batch reaches the maximum batch size or
// maxPutBatchBytes of declared object size. Like [bbolt.DB.Batch], fn may be
// called more than once and is re-run in its own transaction if it fails.
func (db *DB) putBatch(size int, fn func(*bbolt.Tx) error) error {
	c := &db.putCtrl
	c.writes.Add(1)

	errCh := make(chan error, 1)

	c.mtx.Lock()
	if c.pending != nil && c.pending.size+size > maxPutBatchBytes {
		go c.pending.trigger()
		c.pending = nil
	}
	if c.pending == nil {
		c.pending = &putBatch{db: db}
		c.pending.timer = time.AfterFunc(time.Duration(c.txDelay.Load()), c.pending.trigger)
	}
	b := c.pending
	b.calls = append(b.calls, putCall{fn: fn, err: errCh})
	b.size += size
	if len(b.calls) >= db.boltBatchSize || b.size >= maxPutBatchBytes {
		c.pending = nil
		go b.trigger()
	}
	c.mtx.Unlock()

	err := <-errCh
	if errors.Is(err, errPutTrySolo) {
		err = db.boltDB.Update(fn)
	}
	return err
}

func (b *putBatch) trigger() {
	b.start.Do(b.run)
}

func (b *putBatch) run() {
	c := &b.db.putCtrl
	c.commitMtx.Lock()
	defer c.commitMtx.Unlock()

	c.mtx.Lock()
	b.timer.Stop()
	if c.pending == b {
		c.pending = nil
	}
	c.mtx.Unlock()

	var (
		start = time.Now()
		err   error
	)
	for len(b.calls) > 0 {
		failIdx := -1
		err = b.db.boltDB.Update(func(tx *bbolt.Tx) error {
			for i, call := range b.calls {
				if err := safePutCall(call.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})
		if failIdx < 0 {
			break
		}
		call := b.calls[failIdx]
		b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
		call.err <- errPutTrySolo
	}
	if err == nil {
		dur := time.Since(start)
		c.mtx.Lock()
		if c.stats.batches == 0 || dur < c.stats.min {
			c.stats.min = dur
		}
		c.stats.max = max(c.stats.max, dur)
		c.stats.batches++
		c.stats.objects += uint64(len(b.calls))
		c.stats.total += dur
		c.mtx.Unlock()
	}
	for _, call := range b.calls {
		call.err <- err
	}
}

func safePutCall(fn func(*bbolt.Tx) error, tx *bbolt.Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic while putting object: %v", p)
		}
	}()
	return fn(tx)
}

// writeOutsideController logs a write that bypasses the PUT write controller
// and performs it with the given bbolt write method.
func (db *DB) writeOutsideController(op string, objects int, write func(func(*bbolt.Tx) error) error, fn func(*bbolt.Tx) error) error {
	fields := []zap.Field{zap.String("op", op)}
	if objects > 0 {
		fields = append(fields, zap.Int("objects", objects))
	}
	db.log.Info("metabase write outside put controller", fields...)
	return write(fn)
}
