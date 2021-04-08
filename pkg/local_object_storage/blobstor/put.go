package blobstor

import (
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

	big := b.isBig(data)

	// compress object data
	data = b.compressor(data)

	if big {
		// save object in shallow dir
		return new(PutRes), b.fsTree.Put(prm.obj.Address(), data)
	}

	// save object in blobovnicza
	res, err := b.blobovniczas.put(prm.obj.Address(), data)
	if err != nil {
		return nil, err
	}

	return &PutRes{
		roBlobovniczaID: roBlobovniczaID{
			blobovniczaID: res,
		},
	}, nil
}

// checks if object is "big".
func (b *BlobStor) isBig(data []byte) bool {
	return uint64(len(data)) > b.smallSizeLimit
}
