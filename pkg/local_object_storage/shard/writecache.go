package shard

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// FlushWriteCachePrm represents parameters of a `FlushWriteCache` operation.
type FlushWriteCachePrm struct {
	ignoreErrors bool
}

// SetIgnoreErrors sets the flag to ignore read-errors during flush.
func (p *FlushWriteCachePrm) SetIgnoreErrors(ignore bool) {
	p.ignoreErrors = ignore
}

// errWriteCacheDisabled is returned when an operation on write-cache is performed,
// but write-cache is disabled.
var errWriteCacheDisabled = errors.New("write-cache is disabled")

// FlushWriteCache moves writecache in read-only mode and flushes all data from it.
// After the operation writecache will remain read-only mode.
func (s *Shard) FlushWriteCache(p FlushWriteCachePrm) error {
	if !s.hasWriteCache() {
		return errWriteCacheDisabled
	}

	s.m.RLock()
	defer s.m.RUnlock()

	// To write data to the blobstor we need to write to the blobstor and the metabase.
	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	}
	if s.info.Mode.NoMetabase() {
		return ErrDegradedMode
	}

	if err := s.writeCache.SetMode(mode.ReadOnly); err != nil {
		return err
	}

	return s.writeCache.Flush(p.ignoreErrors)
}
