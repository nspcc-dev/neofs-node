package blobstor

import (
	"io/ioutil"
	"os"
	"path"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *object.Object
}

// PutRes groups resulting values of Put operation.
type PutRes struct{}

// WithObject is a Put option to set object to save.
//
// Option is required.
func (p *PutPrm) WithObject(obj *object.Object) *PutPrm {
	if p != nil {
		p.obj = obj
	}

	return p
}

// Put saves the object in BLOB storage.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm *PutPrm) (*PutRes, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// marshal object
	data, err := prm.obj.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal the object")
	}

	// compress object data
	data = b.compressor(data)

	// save object in fs tree
	return nil, b.fsTree.put(prm.obj.Address(), data)
}

func (t *fsTree) put(addr *objectSDK.Address, data []byte) error {
	p := t.treePath(addr)

	if err := os.MkdirAll(path.Dir(p), t.perm); err != nil {
		return err
	}

	return ioutil.WriteFile(p, data, t.perm)
}
