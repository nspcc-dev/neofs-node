package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// IsSmallPrm groups the parameters of IsSmall operation.
type IsSmallPrm struct {
	addr *addressSDK.Address
}

// IsSmallRes groups the resulting values of IsSmall operation.
type IsSmallRes struct {
	id *blobovnicza.ID
}

// WithAddress is a IsSmall option to set the object address to check.
func (p *IsSmallPrm) WithAddress(addr *addressSDK.Address) *IsSmallPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// BlobovniczaID returns blobovnicza identifier.
func (r *IsSmallRes) BlobovniczaID() *blobovnicza.ID {
	return r.id
}

// IsSmall wraps work with DB.IsSmall method with specified
// address and other parameters by default. Returns only
// the blobovnicza identifier.
func IsSmall(db *DB, addr *addressSDK.Address) (*blobovnicza.ID, error) {
	r, err := db.IsSmall(new(IsSmallPrm).WithAddress(addr))
	if err != nil {
		return nil, err
	}

	return r.BlobovniczaID(), nil
}

// IsSmall returns blobovniczaID for small objects and nil for big objects.
// Small objects stored in blobovnicza instances. Big objects stored in FS by
// shallow path which is calculated from address and therefore it is not
// indexed in metabase.
func (db *DB) IsSmall(prm *IsSmallPrm) (res *IsSmallRes, err error) {
	res = new(IsSmallRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.id, err = db.isSmall(tx, prm.addr)

		return err
	})

	return
}

func (db *DB) isSmall(tx *bbolt.Tx, addr *addressSDK.Address) (*blobovnicza.ID, error) {
	cnr, _ := addr.ContainerID()

	smallBucket := tx.Bucket(smallBucketName(&cnr))
	if smallBucket == nil {
		return nil, nil
	}

	id, _ := addr.ObjectID()

	blobovniczaID := smallBucket.Get(objectKey(&id))
	if len(blobovniczaID) == 0 {
		return nil, nil
	}

	return blobovnicza.NewIDFromBytes(slice.Copy(blobovniczaID)), nil
}
