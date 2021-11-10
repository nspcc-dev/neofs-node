package meta

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.etcd.io/bbolt"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *objectSDK.Address
	raw  bool
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	hdr *object.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *objectSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithRaw is a Get option to set raw flag value. If flag is unset, then Get
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *GetPrm) WithRaw(raw bool) *GetPrm {
	if p != nil {
		p.raw = raw
	}

	return p
}

// Header returns the requested object header.
func (r *GetRes) Header() *object.Object {
	return r.hdr
}

// Get reads the object from DB.
func Get(db *DB, addr *objectSDK.Address) (*object.Object, error) {
	r, err := db.Get(new(GetPrm).WithAddress(addr))
	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}

// GetRaw reads physically stored object from DB.
func GetRaw(db *DB, addr *objectSDK.Address, raw bool) (*object.Object, error) {
	r, err := db.Get(new(GetPrm).WithAddress(addr).WithRaw(raw))
	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}

// Get returns object header for specified address.
func (db *DB) Get(prm *GetPrm) (res *GetRes, err error) {
	res = new(GetRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.hdr, err = db.get(tx, prm.addr, true, prm.raw)

		return err
	})

	return
}

func (db *DB) get(tx *bbolt.Tx, addr *objectSDK.Address, checkGraveyard, raw bool) (*object.Object, error) {
	obj := object.New()
	key := objectKey(addr.ObjectID())
	cid := addr.ContainerID()

	if checkGraveyard {
		switch inGraveyard(tx, addr) {
		case 1:
			return nil, object.ErrNotFound
		case 2:
			return nil, object.ErrAlreadyRemoved
		}
	}

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cid), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check if object is a virtual
	return getVirtualObject(tx, cid, key, raw)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func getVirtualObject(tx *bbolt.Tx, cid *cid.ID, key []byte, raw bool) (*object.Object, error) {
	if raw {
		return nil, getSplitInfoError(tx, cid, key)
	}

	parentBucket := tx.Bucket(parentBucketName(cid))
	if parentBucket == nil {
		return nil, object.ErrNotFound
	}

	relativeLst, err := decodeList(parentBucket.Get(key))
	if err != nil {
		return nil, err
	}

	if len(relativeLst) == 0 { // this should never happen though
		return nil, object.ErrNotFound
	}

	// pick last item, for now there is not difference which address to pick
	// but later list might be sorted so first or last value can be more
	// prioritized to choose
	virtualOID := relativeLst[len(relativeLst)-1]
	data := getFromBucket(tx, primaryBucketName(cid), virtualOID)

	child := object.New()

	err = child.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal child with parent: %w", err)
	}

	if child.GetParent() == nil { // this should never happen though
		return nil, object.ErrNotFound
	}

	return child.GetParent(), nil
}

func getSplitInfoError(tx *bbolt.Tx, cid *cid.ID, key []byte) error {
	splitInfo, err := getSplitInfo(tx, cid, key)
	if err == nil {
		return objectSDK.NewSplitInfoError(splitInfo)
	}

	return object.ErrNotFound
}
