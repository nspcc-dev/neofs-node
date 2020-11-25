package blobstor

import (
	"io/ioutil"
	"os"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// GetBigPrm groups the parameters of GetBig operation.
type GetBigPrm struct {
	address
}

// GetBigRes groups resulting values of GetBig operation.
type GetBigRes struct {
	roObject
}

// ErrObjectNotFound is returns on read operations requested on a missing object.
var ErrObjectNotFound = errors.New("object not found")

// GetBig reads the object from shallow dir of BLOB storage by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
func (b *BlobStor) GetBig(prm *GetBigPrm) (*GetBigRes, error) {
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

	return &GetBigRes{
		roObject: roObject{
			obj: obj,
		},
	}, nil
}

func (t *fsTree) get(addr *objectSDK.Address) ([]byte, error) {
	p := t.treePath(addr)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, errFileNotFound
	}

	return ioutil.ReadFile(p)
}
