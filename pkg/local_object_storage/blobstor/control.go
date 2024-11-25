package blobstor

import (
	"fmt"

	"go.uber.org/zap"
)

// Open opens BlobStor.
func (b *BlobStor) Open(readOnly bool) error {
	b.log.Debug("opening...")

	for i := range b.storage {
		err := b.storage[i].Storage.Open(readOnly)
		if err != nil {
			return fmt.Errorf("open substorage %s: %w", b.storage[i].Storage.Type(), err)
		}
	}
	return nil
}

// Init initializes internal data structures and system resources.
//
// If BlobStor is already initialized, no action is taken.
func (b *BlobStor) Init() error {
	b.log.Debug("initializing...")

	if err := b.compression.Init(); err != nil {
		return err
	}

	for i := range b.storage {
		err := b.storage[i].Storage.Init()
		if err != nil {
			return fmt.Errorf("init substorage %s: %w", b.storage[i].Storage.Type(), err)
		}
	}
	b.inited = true
	return nil
}

// Close releases all internal resources of BlobStor.
func (b *BlobStor) Close() error {
	b.log.Debug("closing...")

	var firstErr error
	for i := range b.storage {
		err := b.storage[i].Storage.Close()
		if err != nil {
			b.log.Info("couldn't close storage", zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}

	err := b.compression.Close()
	if firstErr == nil {
		firstErr = err
	}
	return firstErr
}
