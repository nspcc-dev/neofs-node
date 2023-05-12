package blobovniczatree

import (
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

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
		return res, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return
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
		if res, err := b.getObject(v, prm); err == nil {
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
		return common.GetRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return common.GetRes{}, err
	}

	return b.getObject(blz, prm)
}

// reads object from blobovnicza and returns GetSmallRes.
func (b *Blobovniczas) getObject(blz *blobovnicza.Blobovnicza, prm blobovnicza.GetPrm) (common.GetRes, error) {
	res, err := blz.Get(prm)
	if err != nil {
		return common.GetRes{}, err
	}

	// decompress the data
	data, err := b.compression.Decompress(res.Object())
	if err != nil {
		return common.GetRes{}, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRes{}, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	return common.GetRes{Object: obj, RawData: data}, nil
}
