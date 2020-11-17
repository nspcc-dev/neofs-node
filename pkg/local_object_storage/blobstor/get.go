package blobstor

import (
	"io/ioutil"
	"os"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *objectSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *object.Object
}

// ErrObjectNotFound is returns on read operations requested on a missing object.
var ErrObjectNotFound = errors.New("object not found")

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *objectSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Object returns the requested object.
func (r *GetRes) Object() *object.Object {
	return r.obj
}

// Get reads the object from BLOB storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
func (b *BlobStor) Get(prm *GetPrm) (*GetRes, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

	// get compressed object data
	data, err := b.fsTree.get(prm.addr)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			return nil, ErrObjectNotFound
		}

		return nil, errors.Wrap(err, "could not read object from fs tree")
	}

	data, err = b.decompressor(data)
	if err != nil {
		return nil, errors.Wrap(err, "could not decompress object data")
	}

	// unmarshal the object
	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal the object")
	}

	return &GetRes{
		obj: obj,
	}, nil
}

func (t *fsTree) get(addr *objectSDK.Address) ([]byte, error) {
	p := t.treePath(addr)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, errFileNotFound
	}

	return ioutil.ReadFile(p)
}
