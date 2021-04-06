package blobstor

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"

// FSTree returns file-system tree for big object store.
func (b *BlobStor) DumpInfo() fstree.Info {
	return b.fsTree.Info
}
