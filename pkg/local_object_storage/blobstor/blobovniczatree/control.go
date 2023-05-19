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
		blz, err := b.openBlobovniczaNoCache(p)
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
		blz, _ := b.opened.Get(k)
		if err := blz.Close(); err != nil {
			b.log.Debug("could not close active blobovnicza",
				zap.String("path", k),
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
		return v, nil
	}

	lvlPath := filepath.Dir(p)
	curIndex := u64FromHexString(filepath.Base(p))

	b.activeMtx.RLock()
	defer b.activeMtx.RUnlock()

	active, ok := b.active[lvlPath]
	if ok && active.ind == curIndex {
		return active.blz, nil
	}

	b.lruMtx.Lock()
	defer b.lruMtx.Unlock()

	v, ok = b.opened.Get(p)
	if ok {
		return v, nil
	}

	blz, err := b.openBlobovniczaNoCache(p)
	if err != nil {
		return nil, err
	}

	b.opened.Add(p, blz)

	return blz, nil
}

func (b *Blobovniczas) openBlobovniczaNoCache(p string) (*blobovnicza.Blobovnicza, error) {
	b.openMtx.Lock()
	defer b.openMtx.Unlock()

	blz := blobovnicza.New(append(b.blzOpts,
		blobovnicza.WithReadOnly(b.readOnly),
		blobovnicza.WithPath(filepath.Join(b.rootPath, p)),
	)...)

	if err := blz.Open(); err != nil {
		return nil, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
	}
	return blz, nil
}
