package blobstor

import (
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
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
		return nil, fmt.Errorf("could not marshal the object: %w", err)
	}

	return b.PutRaw(prm.obj.Address(), data)
}

// PutRaw saves already marshaled object in BLOB storage.
func (b *BlobStor) PutRaw(addr *objectSDK.Address, data []byte) (*PutRes, error) {
	big := b.isBig(data)

	// compress object data
	data = b.compressor(data)

	if big {
		// save object in shallow dir
		err := b.fsTree.Put(addr, data)
		if err != nil {
			return nil, err
		}

		storagelog.Write(b.log, storagelog.AddressField(addr), storagelog.OpField("fstree PUT"))

		return new(PutRes), nil
	}

	// save object in blobovnicza
	res, err := b.blobovniczas.put(addr, data)
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
