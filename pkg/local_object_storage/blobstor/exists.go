package blobstor

import (
	"os"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	address
}

// ExistsRes groups resulting values of Exists operation.
type ExistsRes struct {
	exists bool
}

// Exists returns the fact that the object is in BLOB storage.
func (r ExistsRes) Exists() bool {
	return r.exists
}

// Exists checks if object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(prm *ExistsPrm) (*ExistsRes, error) {
	// check presence in shallow dir first (cheaper)
	exists, err := b.existsBig(prm.addr)
	if !exists {
		// TODO: do smth if err != nil

		// check presence in blobovnicza
		exists, err = b.existsSmall(prm.addr)
	}

	if err != nil {
		return nil, err
	}

	return &ExistsRes{
		exists: exists,
	}, err
}

// checks if object is presented in shallow dir.
func (b *BlobStor) existsBig(addr *object.Address) (bool, error) {
	_, err := b.fsTree.exists(addr)
	if errors.Is(err, errFileNotFound) {
		return false, nil
	}

	return err == nil, err
}

// checks if object is presented in blobovnicza.
func (b *BlobStor) existsSmall(addr *object.Address) (bool, error) {
	// TODO: implement
	return false, nil
}

func (t *fsTree) exists(addr *objectSDK.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		err = errFileNotFound
	}

	return p, err
}
