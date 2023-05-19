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
			if !outOfBounds && !blobovnicza.IsErrNotFound(err) {
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
		return common.GetRangeRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return
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
		res, err := b.getObjectRange(v, prm)
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
		return common.GetRangeRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	// open blobovnicza (cached inside)
	blz, err := b.openBlobovnicza(blzPath)
	if err != nil {
		return common.GetRangeRes{}, err
	}

	return b.getObjectRange(blz, prm)
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
	data, err := b.compression.Decompress(res.Object())
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
		return common.GetRangeRes{}, logicerr.Wrap(apistatus.ObjectOutOfRange{})
	}

	return common.GetRangeRes{
		Data: payload[from:to],
	}, nil
}
