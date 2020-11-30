package blobovnicza

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *objectSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *object.Object
}

// SetAddress sets address of the requested object.
func (p *GetPrm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// Object returns the requested object.
func (p *GetRes) Object() *object.Object {
	return p.obj
}

// Get reads the object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns ErrNotFound if requested object is not
// presented in Blobovnicza.
func (b *Blobovnicza) Get(prm *GetPrm) (*GetRes, error) {
	var (
		data    []byte
		addrKey = addressKey(prm.addr)
	)

	if err := b.boltDB.View(func(tx *bbolt.Tx) error {
		return b.iterateBuckets(tx, func(lower, upper uint64, buck *bbolt.Bucket) (bool, error) {
			data = buck.Get(addrKey)

			stop := data != nil

			if stop {
				b.log.Debug("object is found in bucket",
					zap.String("binary size", stringifyByteSize(uint64(len(data)))),
					zap.String("range", stringifyBounds(lower, upper)),
				)
			}

			return stop, nil
		})
	}); err != nil {
		return nil, err
	}

	if data == nil {
		return nil, object.ErrNotFound
	}

	// TODO: add decompression step

	// unmarshal the object
	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal the object")
	}

	return &GetRes{
		obj: obj,
	}, nil
}
