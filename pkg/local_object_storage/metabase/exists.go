package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.etcd.io/bbolt"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr *objectSDK.Address
}

// ExistsRes groups resulting values of Exists operation.
type ExistsRes struct {
	exists bool
}

var ErrLackSplitInfo = errors.New("no split info on parent object")

// WithAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) WithAddress(addr *objectSDK.Address) *ExistsPrm {
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
func Exists(db *DB, addr *objectSDK.Address) (bool, error) {
	r, err := db.Exists(new(ExistsPrm).WithAddress(addr))
	if err != nil {
		return false, err
	}

	return r.Exists(), nil
}

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
func (db *DB) Exists(prm *ExistsPrm) (res *ExistsRes, err error) {
	res = new(ExistsRes)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.exists, err = db.exists(tx, prm.addr)

		return err
	})

	return
}

func (db *DB) exists(tx *bbolt.Tx, addr *objectSDK.Address) (exists bool, err error) {
	// check graveyard first
	switch inGraveyard(tx, addr) {
	case 1:
		return false, object.ErrNotFound
	case 2:
		return false, object.ErrAlreadyRemoved
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

	// if parent bucket is empty, then check if object exists in tombstone bucket
	if inBucket(tx, tombstoneBucketName(addr.ContainerID()), objKey) {
		return true, nil
	}

	// if parent bucket is empty, then check if object exists in storage group bucket
	return inBucket(tx, storageGroupBucketName(addr.ContainerID()), objKey), nil
}

// inGraveyard returns:
//  * 0 if object is not in graveyard;
//  * 1 if object is in graveyard with GC mark;
//  * 2 if object is in graveyard with tombstone.
func inGraveyard(tx *bbolt.Tx, addr *objectSDK.Address) uint8 {
	graveyard := tx.Bucket(graveyardBucketName)
	if graveyard == nil {
		return 0
	}

	val := graveyard.Get(addressKey(addr))
	if val == nil {
		return 0
	}

	if bytes.Equal(val, []byte(inhumeGCMarkValue)) {
		return 1
	}

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
