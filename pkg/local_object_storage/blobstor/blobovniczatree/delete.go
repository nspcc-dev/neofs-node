package blobovniczatree

import (
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// Delete deletes object from blobovnicza tree.
//
// If blobocvnicza ID is specified, only this blobovnicza is processed.
// Otherwise, all Blobovniczas are processed descending weight.
func (b *Blobovniczas) Delete(prm common.DeletePrm) (res common.DeleteRes, err error) {
	if b.readOnly {
		return common.DeleteRes{}, common.ErrReadOnly
	}

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
					logger.FieldString("level", p),
					logger.FieldError(err),
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
				logger.FieldString("path", blzPath),
				logger.FieldError(err),
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
				logger.FieldString("path", blzPath),
				logger.FieldError(err),
			)
		}
	}

	// then object is possibly placed in closed blobovnicza

	// check if it makes sense to try to open the blob
	// (Blobovniczas "after" the active one are empty anyway,
	// and it's pointless to open them).
	if u64FromHexString(filepath.Base(blzPath)) > active.ind {
		b.log.Debug("index is too big", logger.FieldString("path", blzPath))
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

// removes object from blobovnicza and returns common.DeleteRes.
func (b *Blobovniczas) deleteObject(blz *blobovnicza.Blobovnicza, prm blobovnicza.DeletePrm, dp common.DeletePrm) (common.DeleteRes, error) {
	_, err := blz.Delete(prm)
	return common.DeleteRes{}, err
}
