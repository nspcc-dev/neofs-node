package blobovniczatree

import (
	"errors"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Put saves object in the maximum weight blobobnicza.
//
// returns error if could not save object in any blobovnicza.
func (b *Blobovniczas) Put(prm common.PutPrm) (common.PutRes, error) {
	if b.readOnly {
		return common.PutRes{}, common.ErrReadOnly
	}

	if !prm.DontCompress {
		prm.RawData = b.compression.Compress(prm.RawData)
	}

	var putPrm blobovnicza.PutPrm
	putPrm.SetAddress(prm.Address)
	putPrm.SetMarshaledObject(prm.RawData)

	var (
		fn      func(string) (bool, error)
		id      *blobovnicza.ID
		allFull = true
	)

	fn = func(p string) (bool, error) {
		active, err := b.getActivated(p)
		if err != nil {
			b.log.Debug("could not get active blobovnicza",
				logger.FieldError(err),
			)

			return false, nil
		}

		if _, err := active.blz.Put(putPrm); err != nil {
			// check if blobovnicza is full
			if errors.Is(err, blobovnicza.ErrFull) {
				b.log.Debug("blobovnicza overflowed",
					logger.FieldString("path", filepath.Join(p, u64ToHexString(active.ind))),
				)

				if err := b.updateActive(p, &active.ind); err != nil {
					b.log.Debug("could not update active blobovnicza",
						logger.FieldString("level", p),
						logger.FieldError(err),
					)

					return false, nil
				}

				return fn(p)
			}

			allFull = false
			b.log.Debug("could not put object to active blobovnicza",
				logger.FieldString("path", filepath.Join(p, u64ToHexString(active.ind))),
				logger.FieldError(err),
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
		if allFull {
			return common.PutRes{}, common.ErrNoSpace
		}
		return common.PutRes{}, errPutFailed
	}

	return common.PutRes{StorageID: id.Bytes()}, nil
}
