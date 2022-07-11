package blobstor

import (
	"errors"
	"fmt"

	"go.uber.org/zap"
)

// Open opens BlobStor.
func (b *BlobStor) Open(readOnly bool) error {
	b.log.Debug("opening...")

	for i := range b.storage {
		err := b.storage[i].Storage.Open(readOnly)
		if err != nil {
			return err
		}
	}
	return nil
}

// ErrInitBlobovniczas is returned when blobovnicza initialization fails.
var ErrInitBlobovniczas = errors.New("failure on blobovnicza initialization stage")

// Init initializes internal data structures and system resources.
//
// If BlobStor is already initialized, no action is taken.
//
// Returns wrapped ErrInitBlobovniczas on blobovnicza tree's initializaiton failure.
func (b *BlobStor) Init() error {
	b.log.Debug("initializing...")

	if err := b.CConfig.Init(); err != nil {
		return err
	}

	for i := range b.storage {
		err := b.storage[i].Storage.Init()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInitBlobovniczas, err)
		}
	}
	return nil
}

// Close releases all internal resources of BlobStor.
func (b *BlobStor) Close() error {
	b.log.Debug("closing...")

	var firstErr error
	for i := range b.storage {
		err := b.storage[i].Storage.Close()
		if err != nil {
			b.log.Info("couldn't close storage", zap.String("error", err.Error()))
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}

	err := b.CConfig.Close()
	if firstErr == nil {
		firstErr = err
	}
	return firstErr
}
