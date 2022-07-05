package blobovniczatree

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"go.uber.org/zap"
)

// Open opens blobovnicza tree.
func (b *Blobovniczas) Open(readOnly bool) error {
	b.readOnly = readOnly
	return nil
}

// Init initializes blobovnicza tree.
//
// Should be called exactly once.
func (b *Blobovniczas) Init() error {
	b.log.Debug("initializing Blobovnicza's")

	err := b.CConfig.Init()
	if err != nil {
		return err
	}
	b.onClose = append(b.onClose, func() {
		if err := b.CConfig.Close(); err != nil {
			b.log.Debug("can't close zstd compressor", zap.String("err", err.Error()))
		}
	})

	if b.readOnly {
		b.log.Debug("read-only mode, skip blobovniczas initialization...")
		return nil
	}

	return b.Iterate(false, func(p string, blz *blobovnicza.Blobovnicza) error {
		if err := blz.Init(); err != nil {
			return fmt.Errorf("could not initialize blobovnicza structure %s: %w", p, err)
		}

		b.log.Debug("blobovnicza successfully initialized, closing...", zap.String("id", p))

		return nil
	})
}

// closes blobovnicza tree.
func (b *Blobovniczas) Close() error {
	b.activeMtx.Lock()

	b.lruMtx.Lock()

	for p, v := range b.active {
		if err := v.blz.Close(); err != nil {
			b.log.Debug("could not close active blobovnicza",
				zap.String("path", p),
				zap.String("error", err.Error()),
			)
		}
		b.opened.Remove(p)
	}
	for _, k := range b.opened.Keys() {
		v, _ := b.opened.Get(k)
		blz := v.(*blobovnicza.Blobovnicza)
		if err := blz.Close(); err != nil {
			b.log.Debug("could not close active blobovnicza",
				zap.String("path", k.(string)),
				zap.String("error", err.Error()),
			)
		}
		b.opened.Remove(k)
	}

	b.active = make(map[string]blobovniczaWithIndex)

	b.lruMtx.Unlock()

	b.activeMtx.Unlock()

	for i := range b.onClose {
		b.onClose[i]()
	}

	return nil
}
