package blobovniczatree

import (
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"go.uber.org/zap"
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
		return common.DeleteRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
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
		if res, err := b.deleteObject(v, prm, dp); err == nil {
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
		return common.DeleteRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
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
