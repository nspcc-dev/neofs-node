package blobstor

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"

// DumpInfo returns information about blob stor.
func (b *BlobStor) DumpInfo() fstree.Info {
	return b.fsTree.Info
}
