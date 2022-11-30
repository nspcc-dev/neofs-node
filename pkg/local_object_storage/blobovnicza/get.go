package blobovnicza

import (
	"errors"
	"runtime/debug"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr oid.Address
}

// GetRes groups the resulting values of Get operation.
type GetRes struct {
	obj []byte
}

// SetAddress sets the address of the requested object.
func (p *GetPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// Object returns binary representation of the requested object.
func (p GetRes) Object() []byte {
	return p.obj
}

// special error for normal bbolt.Tx.ForEach interruption.
var errInterruptForEach = errors.New("interrupt for-each")

// Get reads an object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is not
// presented in Blobovnicza.
func (b *Blobovnicza) Get(prm GetPrm) (res GetRes, err error) {
	var (
		data    []byte
		addrKey = addressKey(prm.addr)
	)

	if err = b.boltDB.View(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

		return tx.ForEach(func(_ []byte, buck *bbolt.Bucket) error {
			data = buck.Get(addrKey)
			if data == nil {
				return nil
			}

			data = slice.Copy(data)

			return errInterruptForEach
		})
	}); err != nil && err != errInterruptForEach {
		return GetRes{}, err
	}

	if data == nil {
		var errNotFound apistatus.ObjectNotFound

		return GetRes{}, errNotFound
	}

	return GetRes{
		obj: data,
	}, nil
}
