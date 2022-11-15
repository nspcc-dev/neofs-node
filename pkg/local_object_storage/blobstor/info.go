package blobstor

// DumpInfo returns information about blob stor.
func (b *BlobStor) DumpInfo() Info {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	sub := make([]SubStorageInfo, len(b.storage))
	for i := range b.storage {
		sub[i].Path = b.storage[i].Storage.Path()
		sub[i].Type = b.storage[i].Storage.Type()
	}

	return Info{
		SubStorages: sub,
	}
}
