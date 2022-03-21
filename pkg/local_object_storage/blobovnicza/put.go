package blobovnicza

import (
	"errors"
	"fmt"

	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	addr *addressSDK.Address

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
func (p *PutPrm) SetAddress(addr *addressSDK.Address) {
	p.addr = addr
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
//
// Should not be called in read-only configuration.
func (b *Blobovnicza) Put(prm *PutPrm) (*PutRes, error) {
	addr := prm.addr
	if addr == nil {
		return nil, errNilAddress
	}

	sz := uint64(len(prm.objData))
	bucketName := bucketForSize(sz)
	key := addressKey(addr)

	err := b.boltDB.Batch(func(tx *bbolt.Tx) error {
		if b.full() {
			return ErrFull
		}

		buck := tx.Bucket(bucketName)
		if buck == nil {
			// expected to happen:
			//  - before initialization step (incorrect usage by design)
			//  - if DB is corrupted (in future this case should be handled)
			return fmt.Errorf("(%T) bucket for size %d not created", b, sz)
		}

		// save the object in bucket
		if err := buck.Put(key, prm.objData); err != nil {
			return fmt.Errorf("(%T) could not save object in bucket: %w", b, err)
		}

		return nil
	})
	if err == nil {
		b.incSize(sz)
	}

	return nil, err
}

func addressKey(addr *addressSDK.Address) []byte {
	return []byte(addr.String())
}

func addressFromKey(dst *addressSDK.Address, data []byte) error {
	return dst.Parse(string(data))
}
