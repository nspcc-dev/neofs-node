package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// ErrNoPlaceFound is returned when object can't be saved to any sub-storage component
// because of the policy.
var ErrNoPlaceFound = logicerr.New("couldn't find a place to store an object")

// Put saves the object in BLOB storage.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in peapod.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm common.PutPrm) (common.PutRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.Object != nil {
		prm.Address = object.AddressOf(prm.Object)
	}
	if prm.RawData == nil {
		// marshal object
		data, err := prm.Object.Marshal()
		if err != nil {
			return common.PutRes{}, fmt.Errorf("could not marshal the object: %w", err)
		}
		prm.RawData = data
	}

	var overflow bool

	for i := range b.storage {
		if b.storage[i].Policy == nil || b.storage[i].Policy(prm.Object, prm.RawData) {
			res, err := b.storage[i].Storage.Put(prm)
			if err != nil {
				if overflow = errors.Is(err, common.ErrNoSpace); overflow {
					b.log.Debug("blobstor sub-storage overflowed, will try another one",
						zap.String("type", b.storage[i].Storage.Type()))
					continue
				}

				return res, fmt.Errorf("put object to sub-storage %s: %w", b.storage[i].Storage.Type(), err)
			}

			logOp(b.log, putOp, prm.Address, b.storage[i].Storage.Type(), res.StorageID)

			return res, nil
		}
	}

	if overflow {
		return common.PutRes{}, common.ErrNoSpace
	}

	return common.PutRes{}, ErrNoPlaceFound
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	return b.cfg.compression.NeedsCompression(obj)
}
