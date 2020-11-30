package blobstor

// Open opens BlobStor.
func (b *BlobStor) Open() error {
	b.log.Debug("opening...")

	return nil
}

// Init initializes internal data structures and system resources.
//
// If BlobStor is already initialized, then no action is taken.
func (b *BlobStor) Init() error {
	b.log.Debug("initializing...")

	return b.blobovniczas.init()
}

// Close releases all internal resources of BlobStor.
func (b *BlobStor) Close() error {
	b.log.Debug("closing...")

	return b.blobovniczas.close()
}
