package blobovnicza

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	addr *objectSDK.Address

	obj *object.Object

	objData []byte
}

// PutRes groups resulting values of Put operation.
type PutRes struct {
}

// ErrFull is returned returned when trying to save an
// object to a filled blobovnicza.
var ErrFull = errors.New("blobovnicza is full")

var errNilAddress = errors.New("object address is nil")

// SetAddress sets address of saving object.
func (p *PutPrm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// SetObject sets the object.
func (p *PutPrm) SetObject(obj *object.Object) {
	p.obj = obj
}

// SetMarshaledObject sets binary representation of the object.
func (p *PutPrm) SetMarshaledObject(data []byte) {
	p.objData = data
}

// Put saves object in Blobovnicza.
//
// If binary representation of the object is not set,
// it is calculated via Marshal method.
//
// The size of the object MUST BE less that or equal to
// the size specified in WithObjectSizeLimit option.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrFull if blobovnicza is filled.
func (b *Blobovnicza) Put(prm *PutPrm) (*PutRes, error) {
	addr := prm.addr
	if addr == nil {
		if addr = prm.obj.Address(); addr == nil {
			return nil, errNilAddress
		}
	}

	err := b.boltDB.Update(func(tx *bbolt.Tx) error {
		if b.full() {
			return ErrFull
		}

		// marshal the object
		data := prm.objData
		if data == nil {
			var err error

			if data, err = prm.obj.Marshal(); err != nil {
				return errors.Wrapf(err, "(%T) could not marshal the object", b)
			}
		}
		// TODO: add compression step

		// calculate size
		sz := uint64(len(data))

		// get bucket for size
		buck := tx.Bucket(bucketForSize(sz))
		if buck == nil {
			// expected to happen:
			//  - before initialization step (incorrect usage by design)
			//  - if DB is corrupted (in future this case should be handled)
			return errors.Errorf("(%T) bucket for size %d not created", b, sz)
		}

		// save the object in bucket
		if err := buck.Put(addressKey(addr), data); err != nil {
			return errors.Wrapf(err, "(%T) could not save object in bucket", b)
		}

		// increase fullness counter
		b.incSize(sz)

		return nil
	})

	return nil, err
}

func addressKey(addr *objectSDK.Address) []byte {
	return []byte(addr.String())
}
