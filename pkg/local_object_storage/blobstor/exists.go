package blobstor

import (
	"errors"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
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

	// If there was an error during existence check below,
	// it will be returned unless object was found in blobovnicza.
	// Otherwise, it is logged and the latest error is returned.
	// FSTree    | Blobovnicza | Behaviour
	// found     | (not tried) | return true, nil
	// not found | any result  | return the result
	// error     | found       | log the error, return true, nil
	// error     | not found   | return the error
	// error     | error       | log the first error, return the second
	if !exists {
		var smallErr error

		exists, smallErr = b.existsSmall(prm.addr)
		if err != nil && (smallErr != nil || exists) {
			b.log.Warn("error occured during object existence checking",
				zap.Stringer("address", prm.addr),
				zap.String("error", err.Error()))
			err = nil
		}
		if err == nil {
			err = smallErr
		}
	}

	if err != nil {
		return nil, err
	}
	return &ExistsRes{exists: exists}, err
}

// checks if object is presented in shallow dir.
func (b *BlobStor) existsBig(addr *addressSDK.Address) (bool, error) {
	_, err := b.fsTree.Exists(addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		return false, nil
	}

	return err == nil, err
}

// existsSmall checks if object is presented in blobovnicza.
func (b *BlobStor) existsSmall(addr *addressSDK.Address) (bool, error) {
	return b.blobovniczas.existsSmall(addr)
}

func (b *blobovniczas) existsSmall(addr *addressSDK.Address) (bool, error) {
	activeCache := make(map[string]struct{})

	prm := new(blobovnicza.GetPrm)
	prm.SetAddress(addr)

	var found bool
	err := b.iterateSortedLeaves(addr, func(p string) (bool, error) {
		dirPath := filepath.Dir(p)

		_, ok := activeCache[dirPath]

		_, err := b.getObjectFromLevel(prm, p, !ok)
		if err != nil {
			if !blobovnicza.IsErrNotFound(err) {
				b.log.Debug("could not get object from level",
					zap.String("level", p),
					zap.String("error", err.Error()))
			}
		}

		activeCache[dirPath] = struct{}{}
		found = err == nil
		return found, nil
	})

	return found, err
}
