package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// IsSmallPrm groups the parameters of IsSmall operation.
type IsSmallPrm struct {
	addr oid.Address
}

// IsSmallRes groups the resulting values of IsSmall operation.
type IsSmallRes struct {
	id *blobovnicza.ID
}

// WithAddress is a IsSmall option to set the object address to check.
func (p *IsSmallPrm) WithAddress(addr oid.Address) {
	if p != nil {
		p.addr = addr
	}
}

// BlobovniczaID returns blobovnicza identifier.
func (r IsSmallRes) BlobovniczaID() *blobovnicza.ID {
	return r.id
}

// IsSmall returns blobovniczaID for small objects and nil for big objects.
// Small objects stored in blobovnicza instances. Big objects stored in FS by
// shallow path which is calculated from address and therefore it is not
// indexed in metabase.
func (db *DB) IsSmall(prm IsSmallPrm) (res IsSmallRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.id, err = db.isSmall(tx, prm.addr)

		return err
	})

	return
}

func (db *DB) isSmall(tx *bbolt.Tx, addr oid.Address) (*blobovnicza.ID, error) {
	smallBucket := tx.Bucket(smallBucketName(addr.Container()))
	if smallBucket == nil {
		return nil, nil
	}

	blobovniczaID := smallBucket.Get(objectKey(addr.Object()))
	if len(blobovniczaID) == 0 {
		return nil, nil
	}

	return blobovnicza.NewIDFromBytes(slice.Copy(blobovniczaID)), nil
}
