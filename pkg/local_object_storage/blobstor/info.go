package blobstor

// DumpInfo returns information about blob stor.
func (b *BlobStor) DumpInfo() Info {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	return Info{
		Path: b.storage.Storage.Path(),
		Type: b.storage.Storage.Type(),
	}
}
