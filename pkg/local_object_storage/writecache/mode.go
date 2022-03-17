package writecache

import (
	"errors"
	"time"
)

// Mode represents write-cache mode of operation.
type Mode uint32

const (
	// ModeReadWrite is a default mode allowing objects to be flushed.
	ModeReadWrite Mode = iota

	// ModeReadOnly is a mode in which write-cache doesn't flush anything to a metabase.
	ModeReadOnly

	// ModeDegraded is similar to a shard's degraded mode.
	ModeDegraded
)

// ErrReadOnly is returned when Put/Write is performed in a read-only mode.
var ErrReadOnly = errors.New("write-cache is in read-only mode")

// SetMode sets write-cache mode of operation.
// When shard is put in read-only mode all objects in memory are flushed to disk
// and all background jobs are suspended.
func (c *cache) SetMode(m Mode) {
	c.modeMtx.Lock()
	defer c.modeMtx.Unlock()
	if c.mode == m {
		return
	}

	c.mode = m
	if m == ModeReadWrite {
		return
	}

	// Because modeMtx is taken no new objects will arrive an all other modifying
	// operations are completed.
	// 1. Persist objects already in memory on disk.
	c.persistMemoryCache()

	// 2. Suspend producers to ensure there are channel send operations in fly.
	// metaCh and directCh can be populated either during Put or in background memory persist thread.
	// Former possibility is eliminated by taking `modeMtx` mutex and
	// latter by explicit persist in the previous step.
	// flushCh is populated by `flush` with `modeMtx` is also taken.
	// Thus all producers are shutdown and we only need to wait until all channels are empty.
	for len(c.metaCh) != 0 || len(c.directCh) != 0 || len(c.flushCh) != 0 {
		c.log.Info("waiting for channels to flush")
		time.Sleep(time.Second)
	}
}

// readOnly returns true if current mode is read-only.
// `c.modeMtx` must be taken.
func (c *cache) readOnly() bool {
	return c.mode != ModeReadWrite
}
