package blobovnicza

import (
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr *addressSDK.Address
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
}

// SetAddress sets the address of the requested object.
func (p *DeletePrm) SetAddress(addr *addressSDK.Address) {
	p.addr = addr
}

// Delete removes an object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely delete the object.
//
// Returns an error of type apistatus.ObjectNotFound if the object to be deleted is not in blobovnicza.
//
// Should not be called in read-only configuration.
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
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	return nil, err
}
