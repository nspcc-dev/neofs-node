package blobstor

import (
	"errors"
	"fmt"
)

// Open opens BlobStor.
func (b *BlobStor) Open() error {
	b.log.Debug("opening...")

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

	err := b.blobovniczas.init()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInitBlobovniczas, err)
	}

	return nil
}

// Close releases all internal resources of BlobStor.
func (b *BlobStor) Close() error {
	b.log.Debug("closing...")

	return b.blobovniczas.close()
}
