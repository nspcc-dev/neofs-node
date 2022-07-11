package blobstor

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"

// DumpInfo returns information about blob stor.
func (b *BlobStor) DumpInfo() fstree.Info {
	for i := range b.storage {
		if b.storage[i].Storage.Type() == "fstree" {
			return b.storage[i].Storage.(*fstree.FSTree).Info
		}
	}
	return fstree.Info{}
}
