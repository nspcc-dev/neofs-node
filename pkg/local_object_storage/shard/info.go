package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
)

// Info groups the information about Shard.
type Info struct {
	// Identifier of the shard.
	ID *ID

	// Shard mode.
	Mode mode.Mode

	// Information about the metabase.
	MetaBaseInfo meta.Info

	// Information about the storage.
	BlobStorInfo StorageInfo

	// Information about the Write Cache.
	WriteCacheInfo writecache.Info

	// ErrorCount contains amount of errors occurred in shard operations.
	ErrorCount uint32
}

// StorageInfo contains information about storage component.
type StorageInfo struct {
	Type string
	Path string
}

// DumpInfo returns information about the Shard.
func (s *Shard) DumpInfo() Info {
	return s.info
}
