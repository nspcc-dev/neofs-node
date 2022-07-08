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
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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
// 1. at each level, according to HRW, the next one is selected and
//   dives into it until we reach the deepest;
// 2. at the B-s level object is saved to the active B. If active B
//   is full, next B is opened, initialized and cached. If there
//   is no more X candidates, goto 1 and process next level.
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

	onClose []func()
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
func (b *Blobovniczas) Put(prm common.PutPrm) (common.PutRes, error) {
	if !prm.DontCompress {
		prm.RawData = b.CConfig.Compress(prm.RawData)
	}

	var putPrm blobovnicza.PutPrm
	putPrm.SetAddress(prm.Address)
	putPrm.SetMarshaledObject(prm.RawData)

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

		if _, err := active.blz.Put(putPrm); err != nil {
			// check if blobovnicza is full
			if errors.Is(err, blobovnicza.ErrFull) {
				b.log.Debug("blobovnicza overflowed",
					zap.String("path", filepath.Join(p, u64ToHexString(active.ind))),
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
				zap.String("path", filepath.Join(p, u64ToHexString(active.ind))),
				zap.String("error", err.Error()),
			)

			return false, nil
		}

		p = filepath.Join(p, u64ToHexString(active.ind))

		id = blobovnicza.NewIDFromBytes([]byte(p))

		return true, nil
	}

	if err := b.iterateDeepest(prm.Address, fn); err != nil {
		return common.PutRes{}, err
	} else if id == nil {
		return common.PutRes{}, errPutFailed
	}

	return common.PutRes{StorageID: id.Bytes()}, nil
}

// Get reads object from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all Blobovniczas are processed descending weight.
func (b *Blobovniczas) Get(prm common.GetPrm) (res common.GetRes, err error) {
	var bPrm blobovnicza.GetPrm
	bPrm.SetAddress(prm.Address)

	if prm.StorageID != nil {
		id := blobovnicza.NewIDFromBytes(prm.StorageID)
		blz, err := b.openBlobovnicza(id.String())
		if err != nil {
			return res, err
		}

		return b.getObject(blz, bPrm)
	}

	activeCache := make(map[string]struct{})

	err = b.iterateSortedLeaves(&prm.Address, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		_, ok := activeCache[dirPath]

		res, err = b.getObjectFromLevel(bPrm, p, !ok)
		if err != nil {
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not get object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
		}

		activeCache[dirPath] = struct{}{}

		// abort iterator if found, otherwise process all Blobovniczas
		return err == nil, nil
	})

	if err == nil && res.Object == nil {
		// not found in any blobovnicza
		var errNotFound apistatus.ObjectNotFound

		return res, errNotFound
	}

	return
}

// Delete deletes object from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all Blobovniczas are processed descending weight.
func (b *Blobovniczas) Delete(prm common.DeletePrm) (res common.DeleteRes, err error) {
	var bPrm blobovnicza.DeletePrm
	bPrm.SetAddress(prm.Address)

	if prm.StorageID != nil {
		id := blobovnicza.NewIDFromBytes(prm.StorageID)
		blz, err := b.openBlobovnicza(id.String())
		if err != nil {
			return res, err
		}

		return b.deleteObject(blz, bPrm, prm)
	}

	activeCache := make(map[string]struct{})
	objectFound := false

	err = b.iterateSortedLeaves(&prm.Address, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		// don't process active blobovnicza of the level twice
		_, ok := activeCache[dirPath]

		res, err = b.deleteObjectFromLevel(bPrm, p, !ok, prm)
		if err != nil {
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not remove object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
		}

		activeCache[dirPath] = struct{}{}

		if err == nil {
			objectFound = true
		}

		// abort iterator if found, otherwise process all Blobovniczas
		return err == nil, nil
	})

	if err == nil && !objectFound {
		// not found in any blobovnicza
		var errNotFound apistatus.ObjectNotFound

		return common.DeleteRes{}, errNotFound
	}

	return
}

// GetRange reads range of object payload data from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all Blobovniczas are processed descending weight.
func (b *Blobovniczas) GetRange(prm common.GetRangePrm) (res common.GetRangeRes, err error) {
	if prm.StorageID != nil {
		id := blobovnicza.NewIDFromBytes(prm.StorageID)
		blz, err := b.openBlobovnicza(id.String())
		if err != nil {
			return common.GetRangeRes{}, err
		}

		return b.getObjectRange(blz, prm)
	}

	activeCache := make(map[string]struct{})
	objectFound := false

	err = b.iterateSortedLeaves(&prm.Address, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		_, ok := activeCache[dirPath]

		res, err = b.getRangeFromLevel(prm, p, !ok)
		if err != nil {
			outOfBounds := isErrOutOfRange(err)
			if !blobovnicza.IsErrNotFound(err) && !outOfBounds {
				b.log.Debug("could not get object from level",
					zap.String("level", p),
					zap.String("error", err.Error()),
				)
			}
			if outOfBounds {
				return true, err
			}
		}

		activeCache[dirPath] = struct{}{}

		objectFound = err == nil

		// abort iterator if found, otherwise process all Blobovniczas
		return err == nil, nil
	})

	if err == nil && !objectFound {
		// not found in any blobovnicza
		var errNotFound apistatus.ObjectNotFound

		return common.GetRangeRes{}, errNotFound
	}

	return
}

// tries to delete object from particular blobovnicza.
//
// returns no error if object was removed from some blobovnicza of the same level.
func (b *Blobovniczas) deleteObjectFromLevel(prm blobovnicza.DeletePrm, blzPath string, tryActive bool, dp common.DeletePrm) (common.DeleteRes, error) {
	lvlPath := filepath.Dir(blzPath)

	// try to remove from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		if res, err := b.deleteObject(v.(*blobovnicza.Blobovnicza), prm, dp); err == nil {
			return res, err
		} else if !blobovnicza.IsErrNotFound(err) {
			b.log.Debug("could not remove object from opened blobovnicza",
				zap.String("path", blzPath),
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
		} else if !blobovnicza.IsErrNotFound(err) {
			b.log.Debug("could not remove object from active blobovnicza",
				zap.String("path", blzPath),
				zap.String("error", err.Error()),
			)
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (Blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(filepath.Base(blzPath)) > active.ind {
		b.log.Debug("index is too big", zap.String("path", blzPath))
		var errNotFound apistatus.ObjectNotFound

		return common.DeleteRes{}, errNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return common.DeleteRes{}, err
	}

	return b.deleteObject(blz, prm, dp)
}

// tries to read object from particular blobovnicza.
//
// returns error if object could not be read from any blobovnicza of the same level.
func (b *Blobovniczas) getObjectFromLevel(prm blobovnicza.GetPrm, blzPath string, tryActive bool) (common.GetRes, error) {
	lvlPath := filepath.Dir(blzPath)

	// try to read from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		if res, err := b.getObject(v.(*blobovnicza.Blobovnicza), prm); err == nil {
			return res, err
		} else if !blobovnicza.IsErrNotFound(err) {
			b.log.Debug("could not read object from opened blobovnicza",
				zap.String("path", blzPath),
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
		} else if !blobovnicza.IsErrNotFound(err) {
			b.log.Debug("could not get object from active blobovnicza",
				zap.String("path", blzPath),
				zap.String("error", err.Error()),
			)
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (Blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(filepath.Base(blzPath)) > active.ind {
		b.log.Debug("index is too big", zap.String("path", blzPath))
		var errNotFound apistatus.ObjectNotFound

		return common.GetRes{}, errNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return common.GetRes{}, err
	}

	return b.getObject(blz, prm)
}

// tries to read range of object payload data from particular blobovnicza.
//
// returns error if object could not be read from any blobovnicza of the same level.
func (b *Blobovniczas) getRangeFromLevel(prm common.GetRangePrm, blzPath string, tryActive bool) (common.GetRangeRes, error) {
	lvlPath := filepath.Dir(blzPath)

	// try to read from blobovnicza if it is opened
	b.lruMtx.Lock()
	v, ok := b.opened.Get(blzPath)
	b.lruMtx.Unlock()
	if ok {
		res, err := b.getObjectRange(v.(*blobovnicza.Blobovnicza), prm)
		switch {
		case err == nil,
			isErrOutOfRange(err):
			return res, err
		default:
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not read payload range from opened blobovnicza",
					zap.String("path", blzPath),
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
			isErrOutOfRange(err):
			return res, err
		default:
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not read payload range from active blobovnicza",
					zap.String("path", blzPath),
					zap.String("error", err.Error()),
				)
			}
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (Blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(filepath.Base(blzPath)) > active.ind {
		b.log.Debug("index is too big", zap.String("path", blzPath))

		var errNotFound apistatus.ObjectNotFound

		return common.GetRangeRes{}, errNotFound
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return common.GetRangeRes{}, err
	}

	return b.getObjectRange(blz, prm)
}

// removes object from blobovnicza and returns common.DeleteRes.
func (b *Blobovniczas) deleteObject(blz *blobovnicza.Blobovnicza, prm blobovnicza.DeletePrm, dp common.DeletePrm) (common.DeleteRes, error) {
	_, err := blz.Delete(prm)
	return common.DeleteRes{}, err
}

// reads object from blobovnicza and returns GetSmallRes.
func (b *Blobovniczas) getObject(blz *blobovnicza.Blobovnicza, prm blobovnicza.GetPrm) (common.GetRes, error) {
	res, err := blz.Get(prm)
	if err != nil {
		return common.GetRes{}, err
	}

	// decompress the data
	data, err := b.Decompress(res.Object())
	if err != nil {
		return common.GetRes{}, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRes{}, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	return common.GetRes{Object: obj}, nil
}

// reads range of object payload data from blobovnicza and returns GetRangeSmallRes.
func (b *Blobovniczas) getObjectRange(blz *blobovnicza.Blobovnicza, prm common.GetRangePrm) (common.GetRangeRes, error) {
	var gPrm blobovnicza.GetPrm
	gPrm.SetAddress(prm.Address)

	// we don't use GetRange call for now since blobovnicza
	// stores data that is compressed on BlobStor side.
	// If blobovnicza learns to do the compression itself,
	// we can start using GetRange.
	res, err := blz.Get(gPrm)
	if err != nil {
		return common.GetRangeRes{}, err
	}

	// decompress the data
	data, err := b.Decompress(res.Object())
	if err != nil {
		return common.GetRangeRes{}, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRangeRes{}, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	from := prm.Range.GetOffset()
	to := from + prm.Range.GetLength()
	payload := obj.Payload()

	if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
		var errOutOfRange apistatus.ObjectOutOfRange

		return common.GetRangeRes{}, errOutOfRange
	}

	return common.GetRangeRes{
		Data: payload[from:to],
	}, nil
}

// iterator over the paths of Blobovniczas in random order.
func (b *Blobovniczas) iterateLeaves(f func(string) (bool, error)) error {
	return b.iterateSortedLeaves(nil, f)
}

// Iterate iterates over all objects in b.
func (b *Blobovniczas) Iterate(prm common.IteratePrm) (common.IterateRes, error) {
	return common.IterateRes{}, b.iterateBlobovniczas(prm.IgnoreErrors, func(p string, blz *blobovnicza.Blobovnicza) error {
		return blobovnicza.IterateObjects(blz, func(addr oid.Address, data []byte) error {
			data, err := b.Decompress(data)
			if err != nil {
				if prm.IgnoreErrors {
					if prm.ErrorHandler != nil {
						return prm.ErrorHandler(addr, err)
					}
					return nil
				}
				return fmt.Errorf("could not decompress object data: %w", err)
			}

			return prm.Handler(common.IterationElement{
				Address:    addr,
				ObjectData: data,
				StorageID:  []byte(p),
			})
		})
	})
}

// iterator over all Blobovniczas in unsorted order. Break on f's error return.
func (b *Blobovniczas) iterateBlobovniczas(ignoreErrors bool, f func(string, *blobovnicza.Blobovnicza) error) error {
	return b.iterateLeaves(func(p string) (bool, error) {
		blz, err := b.openBlobovnicza(p)
		if err != nil {
			if ignoreErrors {
				return false, nil
			}
			return false, fmt.Errorf("could not open blobovnicza %s: %w", p, err)
		}

		err = f(p, blz)

		return err != nil, err
	})
}

// iterator over the paths of Blobovniczas sorted by weight.
func (b *Blobovniczas) iterateSortedLeaves(addr *oid.Address, f func(string) (bool, error)) error {
	_, err := b.iterateSorted(
		addr,
		make([]string, 0, b.blzShallowDepth),
		b.blzShallowDepth,
		func(p []string) (bool, error) { return f(filepath.Join(p...)) },
	)

	return err
}

// iterator over directories with Blobovniczas sorted by weight.
func (b *Blobovniczas) iterateDeepest(addr oid.Address, f func(string) (bool, error)) error {
	depth := b.blzShallowDepth
	if depth > 0 {
		depth--
	}

	_, err := b.iterateSorted(
		&addr,
		make([]string, 0, depth),
		depth,
		func(p []string) (bool, error) { return f(filepath.Join(p...)) },
	)

	return err
}

// iterator over particular level of directories.
func (b *Blobovniczas) iterateSorted(addr *oid.Address, curPath []string, execDepth uint64, f func([]string) (bool, error)) (bool, error) {
	indices := indexSlice(b.blzShallowWidth)

	hrw.SortSliceByValue(indices, addressHash(addr, filepath.Join(curPath...)))

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
		blobovnicza.WithReadOnly(b.readOnly),
		blobovnicza.WithPath(filepath.Join(b.rootPath, p)),
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
