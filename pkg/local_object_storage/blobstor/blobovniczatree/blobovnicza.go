package blobovniczatree

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/nspcc-dev/hrw"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Blobovniczas represents the storage of the "small" objects.
//
// Each object is stored in Blobovnicza's (B-s).
// B-s are structured in a multilevel directory hierarchy
// with fixed depth and width (configured by BlobStor).
//
// Example (width = 4, depth = 3):
//
// x===============================x
// |[0]    [1]    [2]    [3]|
// |   \                /   |
// |    \              /    |
// |     \            /     |
// |      \          /      |
// |[0]    [1]    [2]    [3]|
// |        |    /          |
// |        |   /           |
// |        |  /            |
// |        | /             |
// |[0](F) [1](A) [X]    [X]|
// x===============================x
//
// Elements of the deepest level are B-s.
// B-s are allocated dynamically. At each moment of the time there is
// an active B (ex. A), set of already filled B-s (ex. F) and
// a list of not yet initialized B-s (ex. X). After filling the active B
// it becomes full, and next B becomes initialized and active.
//
// Active B and some of the full B-s are cached (LRU). All cached
// B-s are intitialized and opened.
//
// Object is saved as follows:
//  1. at each level, according to HRW, the next one is selected and
//     dives into it until we reach the deepest;
//  2. at the B-s level object is saved to the active B. If active B
//     is full, next B is opened, initialized and cached. If there
//     is no more X candidates, goto 1 and process next level.
//
// After the object is saved in B, path concatenation is returned
// in system path format as B identifier (ex. "0/1/1" or "3/2/1").
type Blobovniczas struct {
	cfg

	// cache of opened filled Blobovniczas
	opened *simplelru.LRU
	// lruMtx protects opened cache.
	// It isn't RWMutex because `Get` calls must
	// lock this mutex on write, as LRU info is updated.
	// It must be taken after activeMtx in case when eviction is possible
	// i.e. `Add`, `Purge` and `Remove` calls.
	lruMtx sync.Mutex

	// mutex to exclude parallel bbolt.Open() calls
	// bbolt.Open() deadlocks if it tries to open already opened file
	openMtx sync.Mutex

	// list of active (opened, non-filled) Blobovniczas
	activeMtx sync.RWMutex
	active    map[string]blobovniczaWithIndex
}

type blobovniczaWithIndex struct {
	ind uint64

	blz *blobovnicza.Blobovnicza
}

var _ common.Storage = (*Blobovniczas)(nil)

var errPutFailed = errors.New("could not save the object in any blobovnicza")

// NewBlobovniczaTree returns new instance of blobovnizas tree.
func NewBlobovniczaTree(opts ...Option) (blz *Blobovniczas) {
	blz = new(Blobovniczas)
	initConfig(&blz.cfg)

	for i := range opts {
		opts[i](&blz.cfg)
	}

	cache, err := simplelru.NewLRU(blz.openedCacheSize, func(key interface{}, value interface{}) {
		if _, ok := blz.active[filepath.Dir(key.(string))]; ok {
			return
		} else if err := value.(*blobovnicza.Blobovnicza).Close(); err != nil {
			blz.log.Error("could not close Blobovnicza",
				zap.String("id", key.(string)),
				zap.String("error", err.Error()),
			)
		} else {
			blz.log.Debug("blobovnicza successfully closed on evict",
				zap.String("id", key.(string)),
			)
		}
	})
	if err != nil {
		// occurs only if the size is not positive
		panic(fmt.Errorf("could not create LRU cache of size %d: %w", blz.openedCacheSize, err))
	}

	cp := uint64(1)
	for i := uint64(0); i < blz.blzShallowDepth; i++ {
		cp *= blz.blzShallowWidth
	}

	blz.opened = cache
	blz.active = make(map[string]blobovniczaWithIndex, cp)

	return blz
}

// activates and returns activated blobovnicza of p-level (dir).
//
// returns error if blobvnicza could not be activated.
func (b *Blobovniczas) getActivated(p string) (blobovniczaWithIndex, error) {
	return b.updateAndGet(p, nil)
}

// updates active blobovnicza of p-level (dir).
//
// if current active blobovnicza's index is not old, it remains unchanged.
func (b *Blobovniczas) updateActive(p string, old *uint64) error {
	b.log.Debug("updating active blobovnicza...", zap.String("path", p))

	_, err := b.updateAndGet(p, old)

	b.log.Debug("active blobovnicza successfully updated", zap.String("path", p))

	return err
}

// updates and returns active blobovnicza of p-level (dir).
//
// if current active blobovnicza's index is not old, it is returned unchanged.
func (b *Blobovniczas) updateAndGet(p string, old *uint64) (blobovniczaWithIndex, error) {
	b.activeMtx.RLock()
	active, ok := b.active[p]
	b.activeMtx.RUnlock()

	if ok {
		if old != nil {
			if active.ind == b.blzShallowWidth-1 {
				return active, errors.New("no more Blobovniczas")
			} else if active.ind != *old {
				// sort of CAS in order to control concurrent
				// updateActive calls
				return active, nil
			}
		} else {
			return active, nil
		}

		active.ind++
	}

	var err error
	if active.blz, err = b.openBlobovnicza(filepath.Join(p, u64ToHexString(active.ind))); err != nil {
		return active, err
	}

	b.activeMtx.Lock()
	defer b.activeMtx.Unlock()

	// check 2nd time to find out if it blobovnicza was activated while thread was locked
	if tryActive, ok := b.active[p]; ok && tryActive.blz == active.blz {
		return tryActive, nil
	}

	// remove from opened cache (active blobovnicza should always be opened)
	b.lruMtx.Lock()
	b.opened.Remove(p)
	b.lruMtx.Unlock()
	b.active[p] = active

	b.log.Debug("blobovnicza successfully activated",
		zap.String("path", filepath.Join(p, u64ToHexString(active.ind))),
	)

	return active, nil
}

// returns hash of the object address.
func addressHash(addr *oid.Address, path string) uint64 {
	var a string

	if addr != nil {
		a = addr.EncodeToString()
	}

	return hrw.Hash([]byte(a + path))
}

// converts uint64 to hex string.
func u64ToHexString(ind uint64) string {
	return strconv.FormatUint(ind, 16)
}

// converts uint64 hex string to uint64.
func u64FromHexString(str string) uint64 {
	v, err := strconv.ParseUint(str, 16, 64)
	if err != nil {
		panic(fmt.Sprintf("blobovnicza name is not an index %s", str))
	}

	return v
}

// Type implements common.Storage.
func (b *Blobovniczas) Type() string {
	return "blobovniczas"
}

// SetCompressor implements common.Storage.
func (b *Blobovniczas) SetCompressor(cc *compression.Config) {
	b.compression = cc
}
