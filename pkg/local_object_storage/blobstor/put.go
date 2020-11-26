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
	rwObject
}

// PutRes groups resulting values of Put operation.
type PutRes struct {
	roBlobovniczaID
}

// Put saves the object in BLOB storage.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in blobonicza. In this
// case the identifier of blobovnicza is returned.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm *PutPrm) (*PutRes, error) {
	// marshal object
	data, err := prm.obj.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal the object")
	}

	// compress object data
	data = b.compressor(data)

	if b.isBig(prm.obj) {
		// save object in shallow dir
		return nil, b.fsTree.put(prm.obj.Address(), data)
	} else {
		// save object in blobovnicza

		// FIXME: use Blobovnicza when it becomes implemented.
		//  Temporary save in shallow dir.
		return nil, b.fsTree.put(prm.obj.Address(), data)
	}
}

func (t *fsTree) put(addr *objectSDK.Address, data []byte) error {
	p := t.treePath(addr)

	if err := os.MkdirAll(path.Dir(p), t.Permissions); err != nil {
		return err
	}

	return ioutil.WriteFile(p, data, t.Permissions)
}

// checks if object is "big".
func (b *BlobStor) isBig(obj *object.Object) bool {
	// FIXME: headers are temporary ignored
	//  due to slight impact in size.
	//  In fact, we have to honestly calculate the size
	//  in binary format, which requires Size() method.
	sz := obj.PayloadSize()

	return sz > b.smallSizeLimit
}
