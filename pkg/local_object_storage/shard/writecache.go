package shard

import (
	"errors"
)

// errWriteCacheDisabled is returned when an operation on write-cache is performed,
// but write-cache is disabled.
var errWriteCacheDisabled = errors.New("write-cache is disabled")

// FlushWriteCache flushes all data from the write-cache. If ignoreErrors
// is set will flush all objects it can irrespective of any errors.
func (s *Shard) FlushWriteCache(ignoreErrors bool) error {
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

	return s.writeCache.Flush(ignoreErrors)
}
