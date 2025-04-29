package blobstor

import (
	"go.uber.org/zap"
)

// Open opens BlobStor.
func (b *BlobStor) Open(readOnly bool) error {
	b.log.Debug("opening...")

	return b.storage.Storage.Open(readOnly)
}

// Init initializes internal data structures and system resources.
//
// If BlobStor is already initialized, no action is taken.
func (b *BlobStor) Init() error {
	b.log.Debug("initializing...")

	if err := b.compression.Init(); err != nil {
		return err
	}

	err := b.storage.Storage.Init()
	if err != nil {
		return err
	}
	b.inited = true
	return nil
}

// Close releases all internal resources of BlobStor.
func (b *BlobStor) Close() error {
	b.log.Debug("closing...")

	var firstErr error
	err := b.storage.Storage.Close()
	if err != nil {
		b.log.Info("couldn't close storage", zap.Error(err))
		if firstErr == nil {
			firstErr = err
		}
	}

	err = b.compression.Close()
	if firstErr == nil {
		firstErr = err
	}
	return firstErr
}
