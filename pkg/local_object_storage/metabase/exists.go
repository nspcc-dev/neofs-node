package meta

import (
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	res.exists, err = db.exists(prm.addr)

	return
}

func (db *DB) exists(addr *objectSDK.Address) (exists bool, err error) {
	// check graveyard first
	if db.inGraveyard(addr.String()) {
		return false, object.ErrAlreadyRemoved
	}

	objKey := objectKey(addr.ObjectID())
	cidKey := cidBucketKey(addr.ContainerID(), primaryPrefix, objKey)

	// if graveyard is empty, then check if object exists in primary bucket
	if db.hasKey(cidKey) {
		return true, nil
	}

	// if primary bucket is empty, then check if object exists in parent bucket
	cidKey[0] = parentPrefix
	iter := db.newPrefixIterator(cidKey)
	defer iter.Close()

	if iter.First() {
		splitInfo, err := db.getSplitInfo(addr.ContainerID(), objKey)
		if err != nil {
			return false, err
		}

		return false, objectSDK.NewSplitInfoError(splitInfo)
	}

	// if parent bucket is empty, then check if object exists in tombstone bucket
	cidKey[0] = tombstonePrefix
	if db.hasKey(cidKey) {
		return true, nil
	}

	// if parent bucket is empty, then check if object exists in storage group bucket
	cidKey[0] = storageGroupPrefix
	return db.hasKey(cidKey), nil
}

// inGraveyard returns true if object was marked as removed.
func (db *DB) inGraveyard(addr string) bool {
	key := append([]byte{graveyardPrefix}, addr...)
	return db.hasKey(key)
}

func (db *DB) hasKey(key []byte) bool {
	_, c, err := db.db.Get(key)
	if err != nil {
		return false
	}

	c.Close()
	return true
}

// getSplitInfo returns SplitInfo structure from root index. Returns error
// if there is no `key` record in root index.
func (db *DB) getSplitInfo(cid *cid.ID, key []byte) (*objectSDK.SplitInfo, error) {
	cidKey := cidBucketKey(cid, rootPrefix, key)
	rawSplitInfo, c, err := db.db.Get(cidKey)
	if err != nil {
		return nil, ErrLackSplitInfo
	}
	defer c.Close()

	splitInfo := objectSDK.NewSplitInfo()

	err = splitInfo.Unmarshal(rawSplitInfo)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	return splitInfo, nil
}
