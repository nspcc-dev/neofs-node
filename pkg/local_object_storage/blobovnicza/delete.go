package blobovnicza

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr *objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct {
}

// SetAddress sets address of the requested object.
func (p *DeletePrm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// Delete removes object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely delete the object.
//
// Returns ErrObjectNotFound if the object to be deleted is not in blobovnicza.
func (b *Blobovnicza) Delete(prm *DeletePrm) (*DeleteRes, error) {
	addrKey := addressKey(prm.addr)

	removed := false

	err := b.boltDB.Update(func(tx *bbolt.Tx) error {
		return b.iterateBuckets(tx, func(lower, upper uint64, buck *bbolt.Bucket) (bool, error) {
			objData := buck.Get(addrKey)
			if objData == nil {
				// object is not in bucket => continue iterating
				return false, nil
			}

			sz := uint64(len(objData))

			// decrease fullness counter
			b.decSize(sz)

			// remove object from the bucket
			err := buck.Delete(addrKey)

			if err == nil {
				b.log.Debug("object was removed from bucket",
					zap.String("binary size", stringifyByteSize(sz)),
					zap.String("range", stringifyBounds(lower, upper)),
				)
			}

			removed = true

			// stop iteration
			return true, err
		})
	})

	if err == nil && !removed {
		err = ErrObjectNotFound
	}

	return nil, err
}
