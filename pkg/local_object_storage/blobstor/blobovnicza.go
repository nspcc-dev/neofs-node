package blobstor

import (
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/nspcc-dev/hrw"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"go.uber.org/zap"
)

// represents storage of the "small" objects.
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
// list of not yet initialized B-s (ex. X). After filling the active B
// it becomes full, and next B becomes initialized and active.
//
// Active B and some of the full B-s are cached (LRU). All cached
// B-s are intitialized and opened.
//
// Object is saved as follows:
// 1. at each level, according to HRW, the next one is selected and
//   dives into it until we reach the deepest;
// 2. at the B-s level object is saved to the active B. If active B
//   is full, next B is opened, initialized and cached. If there
//   is no more X candidates, goto 1 and process next level.
//
// After the object is saved in B, path concatenation is returned
// in system path format as B identifier (ex. "0/1/1" or "3/2/1").
type blobovniczas struct {
	*cfg

	// cache of opened filled blobovniczas
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

	// list of active (opened, non-filled) blobovniczas
	activeMtx sync.RWMutex
	active    map[string]blobovniczaWithIndex
}

type blobovniczaWithIndex struct {
	ind uint64

	blz *blobovnicza.Blobovnicza
}

var errPutFailed = errors.New("could not save the object in any blobovnicza")

func newBlobovniczaTree(c *cfg) (blz *blobovniczas) {
	cache, err := simplelru.NewLRU(c.openedCacheSize, func(key interface{}, value interface{}) {
		if _, ok := blz.active[path.Dir(key.(string))]; ok {
			return
		} else if err := value.(*blobovnicza.Blobovnicza).Close(); err != nil {
			c.log.Error("could not close Blobovnicza",
				zap.String("id", key.(string)),
				zap.String("error", err.Error()),
			)
		} else {
			c.log.Debug("blobovnicza successfully closed on evict",
				zap.String("id", key.(string)),
			)
		}
	})
	if err != nil {
		// occurs only if the size is not positive
		panic(fmt.Errorf("could not create LRU cache of size %d: %w", c.openedCacheSize, err))
	}

	cp := uint64(1)
	for i := uint64(0); i < c.blzShallowDepth; i++ {
		cp *= c.blzShallowWidth
	}

	return &blobovniczas{
		cfg:    c,
		opened: cache,
		active: make(map[string]blobovniczaWithIndex, cp),
	}
}

// makes slice of uint64 values from 0 to number-1.
func indexSlice(number uint64) []uint64 {
	s := make([]uint64, number)

	for i := range s {
		s[i] = uint64(i)
	}

	return s
}

// save object in the maximum weight blobobnicza.
//
// returns error if could not save object in any blobovnicza.
func (b *blobovniczas) put(addr *objectSDK.Address, data []byte) (*blobovnicza.ID, error) {
	prm := new(blobovnicza.PutPrm)
	prm.SetAddress(addr)
	prm.SetMarshaledObject(data)

	var (
		fn func(string) (bool, error)
		id *blobovnicza.ID
	)

	fn = func(p string) (bool, error) {
		active, err := b.getActivated(p)
		if err != nil {
			b.log.Debug("could not get active blobovnicza",
				zap.String("error", err.Error()),
			)

			return false, nil
		}

		if _, err := active.blz.Put(prm); err != nil {
			// check if blobovnicza is full
			if errors.Is(err, blobovnicza.ErrFull) {
				b.log.Debug("blobovnicza overflowed",
					zap.String("path", path.Join(p, u64ToHexString(active.ind))),
				)

				if err := b.updateActive(p, &active.ind); err != nil {
					b.log.Debug("could not update active blobovnicza",
						zap.String("level", p),
						zap.String("error", err.Error()),
					)

					return false, nil
				}

				return fn(p)
			}

			b.log.Debug("could not put object to active blobovnicza",
				zap.String("path", path.Join(p, u64ToHexString(active.ind))),
				zap.String("error", err.Error()),
			)

			return false, nil
		}

		p = path.Join(p, u64ToHexString(active.ind))

		id = blobovnicza.NewIDFromBytes([]byte(p))

		storagelog.Write(b.log, storagelog.AddressField(addr), storagelog.OpField("blobovniczas PUT"))

		return true, nil
	}

	if err := b.iterateDeepest(addr, fn); err != nil {
		return nil, err
	} else if id == nil {
		return nil, errPutFailed
	}

	return id, nil
}

// reads object from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all blobovniczas are processed descending weight.
func (b *blobovniczas) get(prm *GetSmallPrm) (res *GetSmallRes, err error) {
	bPrm := new(blobovnicza.GetPrm)
	bPrm.SetAddress(prm.addr)

	if prm.blobovniczaID != nil {
		blz, err := b.openBlobovnicza(prm.blobovniczaID.String())
		if err != nil {
			return nil, err
		}

		return b.getObject(blz, bPrm)
	}

	activeCache := make(map[string]struct{})

	err = b.iterateSortedLeaves(prm.addr, func(p string) (bool, error) {
		dirPath := path.Dir(p)

		_, ok := activeCache[dirPath]

		res, err = b.getObjectFromLevel(bPrm, p, !ok)
		if err != nil {
			if !errors.Is(err, object.ErrNotFound) {
				b.log.Debug("could not get object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
		}

		activeCache[dirPath] = struct{}{}

		// abort iterator if found, otherwise process all blobovniczas
		return err == nil, nil
	})

	if err == nil && res == nil {
		// not found in any blobovnicza
		err = object.ErrNotFound
	}

	return
}

// removes object from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all blobovniczas are processed descending weight.
//
// TODO:quite similar to GET, can be unified
func (b *blobovniczas) delete(prm *DeleteSmallPrm) (res *DeleteSmallRes, err error) {
	bPrm := new(blobovnicza.DeletePrm)
	bPrm.SetAddress(prm.addr)

	if prm.blobovniczaID != nil {
		blz, err := b.openBlobovnicza(prm.blobovniczaID.String())
		if err != nil {
			return nil, err
		}

		return b.deleteObject(blz, bPrm, prm)
	}

	activeCache := make(map[string]struct{})

	err = b.iterateSortedLeaves(prm.addr, func(p string) (bool, error) {
		dirPath := path.Dir(p)

		// don't process active blobovnicza of the level twice
		_, ok := activeCache[dirPath]

		res, err = b.deleteObjectFromLevel(bPrm, p, !ok, prm)
		if err != nil {
			if !errors.Is(err, object.ErrNotFound) {
				b.log.Debug("could not remove object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
		}

		activeCache[dirPath] = struct{}{}

		if err == nil {
			res = new(DeleteSmallRes)
		}

		// abort iterator if found, otherwise process all blobovniczas
		return err == nil, nil
	})

	if err == nil && res == nil {
		// not found in any blobovnicza
		err = object.ErrNotFound
	}

	return
}

// reads range of object payload data from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all blobovniczas are processed descending weight.
//
// TODO:quite similar to GET, can be unified
func (b *blobovniczas) getRange(prm *GetRangeSmallPrm) (res *GetRangeSmallRes, err error) {
	if prm.blobovniczaID != nil {
		blz, err := b.openBlobovnicza(prm.blobovniczaID.String())
		if err != nil {
			return nil, err
		}

		return b.getObjectRange(blz, prm)
	}

	activeCache := make(map[string]struct{})

	err = b.iterateSortedLeaves(prm.addr, func(p string) (bool, error) {
		dirPath := path.Dir(p)

		_, ok := activeCache[dirPath]

		res, err = b.getRangeFromLevel(prm, p, !ok)
		if err != nil {
			if !errors.Is(err, object.ErrNotFound) {
				b.log.Debug("could not get object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
		}

		activeCache[dirPath] = struct{}{}

		// abort iterator if found, otherwise process all blobovniczas
		return err == nil, nil
	})

	if err == nil && res == nil {
		// not found in any blobovnicza
		err = object.ErrNotFound
	}

	return
}

// tries to delete object from particular blobovnicza.
//
// returns no error if object was removed from some blobovnicza of the same level.
func (b *blobovniczas) deleteObjectFromLevel(prm *blobovnicza.DeletePrm, blzPath string, tryActive bool, dp *DeleteSmallPrm) (*DeleteSmallRes, error) {
	lvlPath := path.Dir(blzPath)

	log := b.log.With(
		zap.String("path", blzPath),
	)

	// try to remove from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		if res, err := b.deleteObject(v.(*blobovnicza.Blobovnicza), prm, dp); err == nil {
			return res, err
		} else if !errors.Is(err, object.ErrNotFound) {
			log.Debug("could not remove object from opened blobovnicza",
				zap.String("error", err.Error()),
			)
		}
	}

	// therefore the object is possibly placed in a lighter blobovnicza

	// next we check in the active level blobobnicza:
	//  * the active blobovnicza is always opened.
	b.activeMtx.RLock()
	active, ok := b.active[lvlPath]
	b.activeMtx.RUnlock()

	if ok && tryActive {
		if res, err := b.deleteObject(active.blz, prm, dp); err == nil {
			return res, err
		} else if !errors.Is(err, object.ErrNotFound) {
			log.Debug("could not remove object from active blobovnicza",
				zap.String("error", err.Error()),
			)
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(path.Base(blzPath)) > active.ind {
		log.Debug("index is too big")
		return nil, object.ErrNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return nil, err
	}

	return b.deleteObject(blz, prm, dp)
}

// tries to read object from particular blobovnicza.
//
// returns error if object could not be read from any blobovnicza of the same level.
func (b *blobovniczas) getObjectFromLevel(prm *blobovnicza.GetPrm, blzPath string, tryActive bool) (*GetSmallRes, error) {
	lvlPath := path.Dir(blzPath)

	log := b.log.With(
		zap.String("path", blzPath),
	)

	// try to read from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		if res, err := b.getObject(v.(*blobovnicza.Blobovnicza), prm); err == nil {
			return res, err
		} else if !errors.Is(err, object.ErrNotFound) {
			log.Debug("could not read object from opened blobovnicza",
				zap.String("error", err.Error()),
			)
		}
	}

	// therefore the object is possibly placed in a lighter blobovnicza

	// next we check in the active level blobobnicza:
	//  * the freshest objects are probably the most demanded;
	//  * the active blobovnicza is always opened.
	b.activeMtx.RLock()
	active, ok := b.active[lvlPath]
	b.activeMtx.RUnlock()

	if ok && tryActive {
		if res, err := b.getObject(active.blz, prm); err == nil {
			return res, err
		} else if !errors.Is(err, object.ErrNotFound) {
			log.Debug("could not get object from active blobovnicza",
				zap.String("error", err.Error()),
			)
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(path.Base(blzPath)) > active.ind {
		log.Debug("index is too big")
		return nil, object.ErrNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return nil, err
	}

	return b.getObject(blz, prm)
}

// tries to read range of object payload data from particular blobovnicza.
//
// returns error if object could not be read from any blobovnicza of the same level.
func (b *blobovniczas) getRangeFromLevel(prm *GetRangeSmallPrm, blzPath string, tryActive bool) (*GetRangeSmallRes, error) {
	lvlPath := path.Dir(blzPath)

	log := b.log.With(
		zap.String("path", blzPath),
	)

	// try to read from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		res, err := b.getObjectRange(v.(*blobovnicza.Blobovnicza), prm)
		switch {
		case err == nil,
			errors.Is(err, object.ErrRangeOutOfBounds):
			return res, err
		default:
			if !errors.Is(err, object.ErrNotFound) {
				log.Debug("could not read payload range from opened blobovnicza",
					zap.String("error", err.Error()),
				)
			}
		}
	}

	// therefore the object is possibly placed in a lighter blobovnicza

	// next we check in the active level blobobnicza:
	//  * the freshest objects are probably the most demanded;
	//  * the active blobovnicza is always opened.
	b.activeMtx.RLock()
	active, ok := b.active[lvlPath]
	b.activeMtx.RUnlock()

	if ok && tryActive {
		res, err := b.getObjectRange(active.blz, prm)
		switch {
		case err == nil,
			errors.Is(err, object.ErrRangeOutOfBounds):
			return res, err
		default:
			if !errors.Is(err, object.ErrNotFound) {
				log.Debug("could not read payload range from active blobovnicza",
					zap.String("error", err.Error()),
				)
			}
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(path.Base(blzPath)) > active.ind {
		log.Debug("index is too big")
		return nil, object.ErrNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return nil, err
	}

	return b.getObjectRange(blz, prm)
}

// removes object from blobovnicza and returns DeleteSmallRes.
func (b *blobovniczas) deleteObject(blz *blobovnicza.Blobovnicza, prm *blobovnicza.DeletePrm, dp *DeleteSmallPrm) (*DeleteSmallRes, error) {
	_, err := blz.Delete(prm)
	if err != nil {
		return nil, err
	}

	storagelog.Write(b.log,
		storagelog.AddressField(dp.addr),
		storagelog.OpField("blobovniczas DELETE"),
		zap.Stringer("blobovnicza ID", dp.blobovniczaID),
	)

	return new(DeleteSmallRes), nil
}

// reads object from blobovnicza and returns GetSmallRes.
func (b *blobovniczas) getObject(blz *blobovnicza.Blobovnicza, prm *blobovnicza.GetPrm) (*GetSmallRes, error) {
	res, err := blz.Get(prm)
	if err != nil {
		return nil, err
	}

	// decompress the data
	data, err := b.decompressor(res.Object())
	if err != nil {
		return nil, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	return &GetSmallRes{
		roObject: roObject{
			obj: obj,
		},
	}, nil
}

// reads range of object payload data from blobovnicza and returns GetRangeSmallRes.
func (b *blobovniczas) getObjectRange(blz *blobovnicza.Blobovnicza, prm *GetRangeSmallPrm) (*GetRangeSmallRes, error) {
	gPrm := new(blobovnicza.GetPrm)
	gPrm.SetAddress(prm.addr)

	// we don't use GetRange call for now since blobovnicza
	// stores data that is compressed on BlobStor side.
	// If blobovnicza learns to do the compression itself,
	// wecan start using GetRange.
	res, err := blz.Get(gPrm)
	if err != nil {
		return nil, err
	}

	// decompress the data
	data, err := b.decompressor(res.Object())
	if err != nil {
		return nil, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	from := prm.rng.GetOffset()
	to := from + prm.rng.GetLength()
	payload := obj.Payload()

	if uint64(len(payload)) < to {
		return nil, object.ErrRangeOutOfBounds
	}

	return &GetRangeSmallRes{
		rangeData: rangeData{
			data: payload[from:to],
		},
	}, nil
}

// iterator over the paths of blobovniczas in random order.
func (b *blobovniczas) iterateLeaves(f func(string) (bool, error)) error {
	return b.iterateSortedLeaves(nil, f)
}

// iterator over all blobovniczas in unsorted order. Break on f's error return.
func (b *blobovniczas) iterateBlobovniczas(f func(string, *blobovnicza.Blobovnicza) error) error {
	return b.iterateLeaves(func(p string) (bool, error) {
		blz, err := b.openBlobovnicza(p)
		if err != nil {
			return false, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
		}

		err = f(p, blz)

		return err != nil, err
	})
}

// iterator over the paths of blobovniczas sorted by weight.
func (b *blobovniczas) iterateSortedLeaves(addr *objectSDK.Address, f func(string) (bool, error)) error {
	_, err := b.iterateSorted(
		addr,
		make([]string, 0, b.blzShallowDepth),
		b.blzShallowDepth,
		func(p []string) (bool, error) { return f(path.Join(p...)) },
	)

	return err
}

// iterator over directories with blobovniczas sorted by weight.
func (b *blobovniczas) iterateDeepest(addr *objectSDK.Address, f func(string) (bool, error)) error {
	depth := b.blzShallowDepth
	if depth > 0 {
		depth--
	}

	_, err := b.iterateSorted(
		addr,
		make([]string, 0, depth),
		depth,
		func(p []string) (bool, error) { return f(path.Join(p...)) },
	)

	return err
}

// iterator over particular level of directories.
func (b *blobovniczas) iterateSorted(addr *objectSDK.Address, curPath []string, execDepth uint64, f func([]string) (bool, error)) (bool, error) {
	indices := indexSlice(b.blzShallowWidth)

	hrw.SortSliceByValue(indices, addressHash(addr, path.Join(curPath...)))

	exec := uint64(len(curPath)) == execDepth

	for i := range indices {
		if i == 0 {
			curPath = append(curPath, u64ToHexString(indices[i]))
		} else {
			curPath[len(curPath)-1] = u64ToHexString(indices[i])
		}

		if exec {
			if stop, err := f(curPath); err != nil {
				return false, err
			} else if stop {
				return true, nil
			}
		} else if stop, err := b.iterateSorted(addr, curPath, execDepth, f); err != nil {
			return false, err
		} else if stop {
			return true, nil
		}
	}

	return false, nil
}

// activates and returns activated blobovnicza of p-level (dir).
//
// returns error if blobvnicza could not be activated.
func (b *blobovniczas) getActivated(p string) (blobovniczaWithIndex, error) {
	return b.updateAndGet(p, nil)
}

// updates active blobovnicza of p-level (dir).
//
// if current active blobovnicza's index is not old, it remains unchanged.
func (b *blobovniczas) updateActive(p string, old *uint64) error {
	log := b.log.With(zap.String("path", p))

	log.Debug("updating active blobovnicza...")

	_, err := b.updateAndGet(p, old)

	log.Debug("active blobovnicza successfully updated")

	return err
}

// updates and returns active blobovnicza of p-level (dir).
//
// if current active blobovnicza's index is not old, it is returned unchanged.
func (b *blobovniczas) updateAndGet(p string, old *uint64) (blobovniczaWithIndex, error) {
	b.activeMtx.RLock()
	active, ok := b.active[p]
	b.activeMtx.RUnlock()

	if ok {
		if old != nil {
			if active.ind == b.blzShallowWidth-1 {
				return active, errors.New("no more blobovniczas")
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
	if active.blz, err = b.openBlobovnicza(path.Join(p, u64ToHexString(active.ind))); err != nil {
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
		zap.String("path", path.Join(p, u64ToHexString(active.ind))),
	)

	return active, nil
}

// initializes blobovnicza tree.
//
// Should be called exactly once.
func (b *blobovniczas) init() error {
	b.log.Debug("initializing Blobovnicza's")

	return b.iterateBlobovniczas(func(p string, blz *blobovnicza.Blobovnicza) error {
		if err := blz.Init(); err != nil {
			return fmt.Errorf("could not initialize blobovnicza structure %s: %w", p, err)
		}

		log := b.log.With(zap.String("id", p))

		log.Debug("blobovnicza successfully initialized, closing...")

		return nil
	})
}

// closes blobovnicza tree.
func (b *blobovniczas) close() error {
	b.activeMtx.Lock()

	b.lruMtx.Lock()
	b.opened.Purge()
	b.lruMtx.Unlock()

	for p, v := range b.active {
		if err := v.blz.Close(); err != nil {
			b.log.Debug("could not close active blobovnicza",
				zap.String("path", p),
				zap.String("error", err.Error()),
			)
		}

		delete(b.active, p)
	}

	b.activeMtx.Unlock()

	return nil
}

// opens and returns blobovnicza with path p.
//
// If blobovnicza is already opened and cached, instance from cache is returned w/o changes.
func (b *blobovniczas) openBlobovnicza(p string) (*blobovnicza.Blobovnicza, error) {
	b.lruMtx.Lock()
	v, ok := b.opened.Get(p)
	b.lruMtx.Unlock()
	if ok {
		// blobovnicza should be opened in cache
		return v.(*blobovnicza.Blobovnicza), nil
	}

	b.openMtx.Lock()
	defer b.openMtx.Unlock()

	b.lruMtx.Lock()
	v, ok = b.opened.Get(p)
	b.lruMtx.Unlock()
	if ok {
		// blobovnicza should be opened in cache
		return v.(*blobovnicza.Blobovnicza), nil
	}

	blz := blobovnicza.New(append(b.blzOpts,
		blobovnicza.WithPath(path.Join(b.blzRootPath, p)),
	)...)

	if err := blz.Open(); err != nil {
		return nil, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
	}

	b.activeMtx.Lock()
	b.lruMtx.Lock()

	b.opened.Add(p, blz)

	b.lruMtx.Unlock()
	b.activeMtx.Unlock()

	return blz, nil
}

// returns hash of the object address.
func addressHash(addr *objectSDK.Address, path string) uint64 {
	var a string

	if addr != nil {
		a = addr.String()
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
