package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// SetMode sets the blobstor mode of operation.
func (b *BlobStor) SetMode(m mode.Mode) error {
	b.modeMtx.Lock()
	defer b.modeMtx.Unlock()

	if b.mode == m {
		return nil
	}

	if b.mode.ReadOnly() == m.ReadOnly() {
		return nil
	}

	err := b.Close()
	if err == nil {
		if err = b.Open(m.ReadOnly()); err == nil && b.inited {
			err = b.Init()
		}
	}
	if err != nil {
		return fmt.Errorf("can't set blobstor mode (old=%s, new=%s): %w", b.mode, m, err)
	}

	b.mode = m
	return nil
}
