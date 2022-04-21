package meta

import (
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr *addressSDK.Address
}

// ExistsRes groups the resulting values of Exists operation.
type ExistsRes struct {
	exists bool
}

var ErrLackSplitInfo = errors.New("no split info on parent object")

// WithAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) WithAddress(addr *addressSDK.Address) *ExistsPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Exists returns the fact that the object is in the metabase.
func (p *ExistsRes) Exists() bool {
	return p.exists
}

// Exists checks if object is presented in DB.
//
// See DB.Exists docs.
func Exists(db *DB, addr *addressSDK.Address) (bool, error) {
	r, err := db.Exists(new(ExistsPrm).WithAddress(addr))
	if err != nil {
		return false, err
	}

	return r.Exists(), nil
}

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
func (db *DB) Exists(prm *ExistsPrm) (res *ExistsRes, err error) {
	res = new(ExistsRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.exists, err = db.exists(tx, prm.addr)

		return err
	})

	return
}

func (db *DB) exists(tx *bbolt.Tx, addr *addressSDK.Address) (exists bool, err error) {
	// check graveyard first
	switch inGraveyard(tx, addr) {
	case 1:
		var errNotFound apistatus.ObjectNotFound

		return false, errNotFound
	case 2:
		var errRemoved apistatus.ObjectAlreadyRemoved

		return false, errRemoved
	}

	objKey := objectKey(addr.ObjectID())

	// if graveyard is empty, then check if object exists in primary bucket
	if inBucket(tx, primaryBucketName(addr.ContainerID()), objKey) {
		return true, nil
	}

	// if primary bucket is empty, then check if object exists in parent bucket
	if inBucket(tx, parentBucketName(addr.ContainerID()), objKey) {
		splitInfo, err := getSplitInfo(tx, addr.ContainerID(), objKey)
		if err != nil {
			return false, err
		}

		return false, objectSDK.NewSplitInfoError(splitInfo)
	}

	// if parent bucket is empty, then check if object exists in typed buckets
	return firstIrregularObjectType(tx, *addr.ContainerID(), objKey) != objectSDK.TypeRegular, nil
}

// inGraveyard returns:
//  * 0 if object is not marked for deletion;
//  * 1 if object with GC mark;
//  * 2 if object is covered with tombstone.
func inGraveyard(tx *bbolt.Tx, addr *addressSDK.Address) uint8 {
	graveyard := tx.Bucket(graveyardBucketName)
	if graveyard == nil {
		// incorrect metabase state, does not make
		// sense to check garbage bucket
		return 0
	}

	val := graveyard.Get(addressKey(addr))
	if val == nil {
		garbageBCK := tx.Bucket(garbageBucketName)
		if garbageBCK == nil {
			// incorrect node state
			return 0
		}

		val = garbageBCK.Get(addressKey(addr))
		if val != nil {
			// object has been marked with GC
			return 1
		}

		// neither in the graveyard
		// nor was marked with GC mark
		return 0
	}

	// object in the graveyard
	return 2
}

// inBucket checks if key <key> is present in bucket <name>.
func inBucket(tx *bbolt.Tx, name, key []byte) bool {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return false
	}

	// using `get` as `exists`: https://github.com/boltdb/bolt/issues/321
	val := bkt.Get(key)

	return len(val) != 0
}

// getSplitInfo returns SplitInfo structure from root index. Returns error
// if there is no `key` record in root index.
func getSplitInfo(tx *bbolt.Tx, cid *cid.ID, key []byte) (*objectSDK.SplitInfo, error) {
	rawSplitInfo := getFromBucket(tx, rootBucketName(cid), key)
	if len(rawSplitInfo) == 0 {
		return nil, ErrLackSplitInfo
	}

	splitInfo := objectSDK.NewSplitInfo()

	err := splitInfo.Unmarshal(rawSplitInfo)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	return splitInfo, nil
}
