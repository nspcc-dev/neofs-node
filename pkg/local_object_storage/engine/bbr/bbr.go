package bbr

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

// alias to distinguish from byte units.
type blocks = int64

// TODO: comments.
const (
	blockSize               = 64 << 10
	minLimit                = 256 << 20 / blockSize
	initialSpeed            = 100 << 20 / blockSize
	increaseAlpha           = 0.2
	decreaseAlpha           = 0.05
	speedDeviationThreshold = 1 << 20 / blockSize
	targetLatency           = 0.03 // 30ms
)

// TODO: docs.
type LoadLimiter struct {
	speed    atomic.Uint64
	limit    atomic.Uint64
	inflight atomic.Int64
}

// TODO: docs.
func NewLoadLimiter() *LoadLimiter {
	var res LoadLimiter
	res.speed.Store(math.Float64bits(initialSpeed / blockSize))
	return &res
}

// TODO: docs.
func (x *LoadLimiter) Write(sizeBytes int64, writeFn func() error) (bool, error) {
	if sizeBytes < 0 {
		panic(fmt.Sprintf("negative size %d", sizeBytes))
	}
	return x.write(calculateBlocks(sizeBytes), writeFn)
}

func (x *LoadLimiter) write(size blocks, writeFn func() error) (bool, error) {
	if !x.acquire(size) {
		return true, nil
	}

	startTime := time.Now()

	err := writeFn()

	duration := time.Since(startTime)

	x.release(size)

	if err != nil {
		return false, err
	}

	x.handleMeasurement(size, duration)

	return false, nil
}

func (x *LoadLimiter) acquire(size blocks) bool {
	newInflight := x.inflight.Add(size)
	limit := max(x.limit.Load(), minLimit)

	if newInflight > size && uint64(newInflight) > limit {
		x.release(size)
		return false
	}

	return true
}

func (x *LoadLimiter) release(size blocks) {
	x.inflight.Add(-size)
}

func (x *LoadLimiter) handleMeasurement(size blocks, duration time.Duration) {
	if duration <= 0 {
		return
	}

	opSpeed := float64(size) / duration.Seconds()

	prevSpeedBits := x.speed.Load()
	prevSpeed := math.Float64frombits(prevSpeedBits)

	if math.Abs(opSpeed-prevSpeed) < speedDeviationThreshold {
		return
	}

	var alpha float64
	if opSpeed > prevSpeed {
		alpha = increaseAlpha
	} else {
		alpha = decreaseAlpha
	}

	newSpeed := (prevSpeed * (1 - alpha)) + (opSpeed * alpha)

	if !x.speed.CompareAndSwap(prevSpeedBits, math.Float64bits(newSpeed)) {
		return
	}

	limit := uint64(math.Floor(newSpeed * targetLatency))
	x.limit.Store(limit)
}

func calculateBlocks(sizeBytes int64) blocks {
	if sizeBytes <= blockSize {
		return 1
	}
	return (sizeBytes + blockSize - 1) / blockSize
}
