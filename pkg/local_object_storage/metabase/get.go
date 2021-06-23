package meta

import (
	"bytes"
	"fmt"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	res.hdr, err = db.get(prm.addr, true, prm.raw)

	return
}

func (db *DB) get(addr *objectSDK.Address, checkGraveyard, raw bool) (*object.Object, error) {
	return db.getAux(addr, addr.String(), checkGraveyard, raw)
}

func (db *DB) getAux(addr *objectSDK.Address, addrStr string, checkGraveyard, raw bool) (*object.Object, error) {
	if checkGraveyard && db.inGraveyard(addrStr) {
		return nil, object.ErrAlreadyRemoved
	}

	obj := object.New()
	key := objectKey(addr.ObjectID())
	cid := addr.ContainerID()

	value := cidBucketKey(cid, primaryPrefix, key)

	iter := db.newPrefixIterator(nil)
	defer iter.Close()

	// check in primary index
	value[0] = primaryPrefix
	if iter.SeekGE(value) && bytes.Equal(iter.Key(), value) {
		return obj, obj.Unmarshal(iter.Value())
	}

	// if not found then check in tombstone index
	value[0] = tombstonePrefix
	if iter.SeekGE(value) && bytes.Equal(iter.Key(), value) {
		return obj, obj.Unmarshal(iter.Value())
	}

	// if not found then check in storage group index
	value[0] = storageGroupPrefix
	if iter.SeekGE(value) && bytes.Equal(iter.Key(), value) {
		return obj, obj.Unmarshal(iter.Value())
	}

	// if not found then check if object is a virtual
	return db.getVirtualObject(cid, key, raw)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func (db *DB) getVirtualObject(cid *cid.ID, key []byte, raw bool) (*object.Object, error) {
	if raw {
		return nil, db.getSplitInfoError(cid, key)
	}

	cidKey := cidBucketKey(cid, parentPrefix, nil)
	iterKey := appendKey(cidKey, key)
	iter := db.newPrefixIterator(iterKey)
	defer iter.Close()

	// pick last item, for now there is not difference which address to pick
	// but later list might be sorted so first or last value can be more
	// prioritized to choose
	var virtualOID []byte
	for iter.First(); iter.Valid(); iter.Next() {
		virtualOID = iter.Key()[len(iterKey)+1:]
	}

	if virtualOID == nil { // this should never happen though
		return nil, object.ErrNotFound
	}

	cidKey[0] = primaryPrefix
	oidKey := appendKey(cidKey, virtualOID)
	value, c, err := db.db.Get(oidKey)
	if err != nil {
		return nil, object.ErrNotFound
	}
	defer c.Close()

	child := object.New()

	err = child.Unmarshal(value)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal child with parent: %w", err)
	}

	if child.GetParent() == nil { // this should never happen though
		return nil, object.ErrNotFound
	}

	return child.GetParent(), nil
}

func (db *DB) getSplitInfoError(cid *cid.ID, key []byte) error {
	splitInfo, err := db.getSplitInfo(cid, key)
	if err == nil {
		return objectSDK.NewSplitInfoError(splitInfo)
	}

	return object.ErrNotFound
}
