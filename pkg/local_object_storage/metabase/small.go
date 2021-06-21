package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
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

	res.id, err = db.isSmall(prm.addr)

	return
}

func (db *DB) isSmall(addr *objectSDK.Address) (*blobovnicza.ID, error) {
	cidKey := cidBucketKey(addr.ContainerID(), smallPrefix, objectKey(addr.ObjectID()))
	blobovniczaID, c, err := db.db.Get(cidKey)
	if err != nil {
		return nil, nil
	}
	defer c.Close()

	return blobovnicza.NewIDFromBytes(cloneBytes(blobovniczaID)), nil
}

func cloneBytes(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
