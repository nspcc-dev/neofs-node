package blobovniczatree

import (
	"errors"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/cmd/blobovnicza-to-peapod/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
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
			if !isLogical(err) {
				b.reportError("could not get active blobovnicza", err)
			} else {
				b.log.Debug("could not get active blobovnicza",
					zap.String("error", err.Error()))
			}

			return false, nil
		}

		if _, err := active.blz.Put(putPrm); err != nil {
			// Check if blobovnicza is full. We could either receive `blobovnicza.ErrFull` error
			// or update active blobovnicza in other thread. In the latter case the database will be closed
			// and `updateActive` takes care of not updating the active blobovnicza twice.
			if isFull := errors.Is(err, blobovnicza.ErrFull); isFull || errors.Is(err, bbolt.ErrDatabaseNotOpen) {
				if isFull {
					b.log.Debug("blobovnicza overflowed",
						zap.String("path", filepath.Join(p, u64ToHexString(active.ind))))
				}

				if err := b.updateActive(p, &active.ind); err != nil {
					if !isLogical(err) {
						b.reportError("could not update active blobovnicza", err)
					} else {
						b.log.Debug("could not update active blobovnicza",
							zap.String("level", p),
							zap.String("error", err.Error()))
					}

					return false, nil
				}

				return fn(p)
			}

			allFull = false
			if !isLogical(err) {
				b.reportError("could not put object to active blobovnicza", err)
			} else {
				b.log.Debug("could not put object to active blobovnicza",
					zap.String("path", filepath.Join(p, u64ToHexString(active.ind))),
					zap.String("error", err.Error()))
			}

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
