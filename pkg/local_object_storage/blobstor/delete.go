package blobstor

import (
	"os"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr *object.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddress is a Delete option to set the address of the object to delete.
//
// Option is required.
func (p *DeletePrm) WithAddress(addr *object.Address) *DeletePrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Delete removes object from BLOB storage.
//
// Returns any error encountered that did not allow
// to completely remove the object.
func (b *BlobStor) Delete(prm *DeletePrm) (*DeleteRes, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	err := b.fsTree.delete(prm.addr)
	if errors.Is(err, errFileNotFound) {
		err = nil
	}

	return nil, err
}

func (t *fsTree) delete(addr *object.Address) error {
	p := t.treePath(addr)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return errFileNotFound
	}

	return os.Remove(p)
}
