package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.etcd.io/bbolt"
)

// IsSmallPrm groups the parameters of IsSmall operation.
type IsSmallPrm struct {
	addr *objectSDK.Address
}

// IsSmallRes groups resulting values of IsSmall operation.
type IsSmallRes struct {
	id *blobovnicza.ID
}

// WithAddress is a IsSmall option to set the object address to check.
func (p *IsSmallPrm) WithAddress(addr *objectSDK.Address) *IsSmallPrm {
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
func IsSmall(db *DB, addr *objectSDK.Address) (*blobovnicza.ID, error) {
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

func (db *DB) isSmall(tx *bbolt.Tx, addr *objectSDK.Address) (*blobovnicza.ID, error) {
	smallBucket := tx.Bucket(smallBucketName(addr.ContainerID()))
	if smallBucket == nil {
		return nil, nil
	}

	blobovniczaID := smallBucket.Get(objectKey(addr.ObjectID()))
	if len(blobovniczaID) == 0 {
		return nil, nil
	}

	return blobovnicza.NewIDFromBytes(slice.Copy(blobovniczaID)), nil
}
