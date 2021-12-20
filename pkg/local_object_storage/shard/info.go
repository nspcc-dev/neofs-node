package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
)

// Info groups the information about Shard.
type Info struct {
	// Identifier of the shard.
	ID *ID

	// Information about the metabase.
	MetaBaseInfo meta.Info

	// Information about the BLOB storage.
	BlobStorInfo blobstor.Info

	// Information about the Write Cache.
	WriteCacheInfo writecache.Info

	// Weight parameters of the shard.
	WeightValues WeightValues
}

// DumpInfo returns information about the Shard.
func (s *Shard) DumpInfo() Info {
	return s.info
}
