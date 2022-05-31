package meta

import (
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr oid.Address
	raw  bool
}

// GetRes groups the resulting values of Get operation.
type GetRes struct {
	hdr *objectSDK.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr oid.Address) *GetPrm {
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
func (r *GetRes) Header() *objectSDK.Object {
	return r.hdr
}

// Get reads the object from DB.
func Get(db *DB, addr oid.Address) (*objectSDK.Object, error) {
	r, err := db.Get(new(GetPrm).WithAddress(addr))
	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}

// GetRaw reads physically stored object from DB.
func GetRaw(db *DB, addr oid.Address, raw bool) (*objectSDK.Object, error) {
	r, err := db.Get(new(GetPrm).WithAddress(addr).WithRaw(raw))
	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}

// Get returns object header for specified address.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in DB.
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
func (db *DB) Get(prm *GetPrm) (res *GetRes, err error) {
	res = new(GetRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.hdr, err = db.get(tx, prm.addr, true, prm.raw)

		return err
	})

	return
}

func (db *DB) get(tx *bbolt.Tx, addr oid.Address, checkGraveyard, raw bool) (*objectSDK.Object, error) {
	key := objectKey(addr.Object())

	if checkGraveyard {
		switch inGraveyard(tx, addr) {
		case 1:
			var errNotFound apistatus.ObjectNotFound

			return nil, errNotFound
		case 2:
			var errRemoved apistatus.ObjectAlreadyRemoved

			return nil, errRemoved
		}
	}

	cnr := addr.Container()
	obj := objectSDK.New()

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cnr), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cnr), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cnr), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in locker index
	data = getFromBucket(tx, bucketNameLockers(cnr), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check if object is a virtual
	return getVirtualObject(tx, cnr, key, raw)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func getVirtualObject(tx *bbolt.Tx, cnr cid.ID, key []byte, raw bool) (*objectSDK.Object, error) {
	if raw {
		return nil, getSplitInfoError(tx, cnr, key)
	}

	parentBucket := tx.Bucket(parentBucketName(cnr))
	if parentBucket == nil {
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	relativeLst, err := decodeList(parentBucket.Get(key))
	if err != nil {
		return nil, err
	}

	if len(relativeLst) == 0 { // this should never happen though
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	// pick last item, for now there is not difference which address to pick
	// but later list might be sorted so first or last value can be more
	// prioritized to choose
	virtualOID := relativeLst[len(relativeLst)-1]
	data := getFromBucket(tx, primaryBucketName(cnr), virtualOID)

	child := objectSDK.New()

	err = child.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal child with parent: %w", err)
	}

	par := child.Parent()

	if par == nil { // this should never happen though
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	return par, nil
}

func getSplitInfoError(tx *bbolt.Tx, cnr cid.ID, key []byte) error {
	splitInfo, err := getSplitInfo(tx, cnr, key)
	if err == nil {
		return objectSDK.NewSplitInfoError(splitInfo)
	}

	var errNotFound apistatus.ObjectNotFound

	return errNotFound
}
