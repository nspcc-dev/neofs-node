package blobovniczatree

import (
	"fmt"
	"path/filepath"

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

	if b.readOnly {
		b.log.Debug("read-only mode, skip blobovniczas initialization...")
		return nil
	}

	return b.iterateLeaves(func(p string) (bool, error) {
		blz, err := b.openBlobovniczaNoCache(p, false)
		if err != nil {
			return true, err
		}
		defer blz.Close()

		if err := blz.Init(); err != nil {
			return true, fmt.Errorf("could not initialize blobovnicza structure %s: %w", p, err)
		}

		b.log.Debug("blobovnicza successfully initialized, closing...", zap.String("id", p))
		return false, nil
	})
}

// Close implements common.Storage.
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

	return nil
}

// opens and returns blobovnicza with path p.
//
// If blobovnicza is already opened and cached, instance from cache is returned w/o changes.
func (b *Blobovniczas) openBlobovnicza(p string) (*blobovnicza.Blobovnicza, error) {
	b.lruMtx.Lock()
	v, ok := b.opened.Get(p)
	b.lruMtx.Unlock()
	if ok {
		// blobovnicza should be opened in cache
		return v.(*blobovnicza.Blobovnicza), nil
	}

	blz, err := b.openBlobovniczaNoCache(p, true)
	if err != nil {
		return nil, err
	}

	b.activeMtx.Lock()
	b.lruMtx.Lock()

	b.opened.Add(p, blz)

	b.lruMtx.Unlock()
	b.activeMtx.Unlock()

	return blz, nil
}

func (b *Blobovniczas) openBlobovniczaNoCache(p string, tryCache bool) (*blobovnicza.Blobovnicza, error) {
	b.openMtx.Lock()
	defer b.openMtx.Unlock()

	if tryCache {
		b.lruMtx.Lock()
		v, ok := b.opened.Get(p)
		b.lruMtx.Unlock()
		if ok {
			// blobovnicza should be opened in cache
			return v.(*blobovnicza.Blobovnicza), nil
		}
	}

	blz := blobovnicza.New(append(b.blzOpts,
		blobovnicza.WithReadOnly(b.readOnly),
		blobovnicza.WithPath(filepath.Join(b.rootPath, p)),
	)...)

	if err := blz.Open(); err != nil {
		return nil, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
	}
	return blz, nil
}
