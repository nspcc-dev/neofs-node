package blobstor

import (
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get reads the object from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) Get(prm common.GetPrm) (common.GetRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.Get(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return res, err
			}
		}

		return common.GetRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	if len(prm.StorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.Get(prm)
	}
	return b.storage[0].Storage.Get(prm)
}

// OpenObjectStream looks up for referenced object in the BlobStor and, if the
// object exists, opens and returns stream with binary-encoded object. Returns
// [apistatus.ErrObjectNotFound] if object was not found. Resulting stream must
// be finally closed.
func (b *BlobStor) OpenObjectStream(objAddr oid.Address) (io.ReadSeekCloser, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	var errFirstNonMissing error

	for i := range b.storage {
		res, err := b.storage[i].Storage.OpenObjectStream(objAddr)
		if err == nil {
			return res, nil
		}

		if errFirstNonMissing == nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
			errFirstNonMissing = err
		}
	}

	if errFirstNonMissing != nil {
		return nil, errFirstNonMissing
	}

	return nil, apistatus.ErrObjectNotFound
}
