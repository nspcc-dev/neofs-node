package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
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

	// Information about the BLOB storage.
	BlobStorInfo blobstor.Info

	// Information about the Write Cache.
	WriteCacheInfo writecache.Info

	// ErrorCount contains amount of errors occurred in shard operations.
	ErrorCount uint32

	// PiloramaInfo contains information about trees stored on this shard.
	PiloramaInfo pilorama.Info
}

// DumpInfo returns information about the Shard.
func (s *Shard) DumpInfo() Info {
	return s.info
}
