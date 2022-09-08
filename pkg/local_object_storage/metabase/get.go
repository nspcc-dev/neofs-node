package meta

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

// SetAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetRaw is a Get option to set raw flag value. If flag is unset, then Get
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *GetPrm) SetRaw(raw bool) {
	p.raw = raw
}

// Header returns the requested object header.
func (r GetRes) Header() *objectSDK.Object {
	return r.hdr
}

// Get returns object header for specified address.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in DB.
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Get(prm GetPrm) (res GetRes, err error) {
	db.modeMtx.Lock()
	defer db.modeMtx.Unlock()

	currEpoch := db.epochState.CurrentEpoch()

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		key := make([]byte, addressKeySize)
		res.hdr, err = db.get(tx, prm.addr, key, true, prm.raw, currEpoch)

		return err
	})

	return
}

func (db *DB) get(tx *bbolt.Tx, addr oid.Address, key []byte, checkStatus, raw bool, currEpoch uint64) (*objectSDK.Object, error) {
	if checkStatus {
		switch objectStatus(tx, addr, currEpoch) {
		case 1:
			var errNotFound apistatus.ObjectNotFound

			return nil, errNotFound
		case 2:
			var errRemoved apistatus.ObjectAlreadyRemoved

			return nil, errRemoved
		case 3:
			return nil, object.ErrObjectIsExpired
		}
	}

	key = objectKey(addr.Object(), key)
	cnr := addr.Container()
	obj := objectSDK.New()
	bucketName := make([]byte, bucketKeySize)

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in locker index
	data = getFromBucket(tx, bucketNameLockers(cnr, bucketName), key)
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

	bucketName := make([]byte, bucketKeySize)
	parentBucket := tx.Bucket(parentBucketName(cnr, bucketName))
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
	data := getFromBucket(tx, primaryBucketName(cnr, bucketName), virtualOID)

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
