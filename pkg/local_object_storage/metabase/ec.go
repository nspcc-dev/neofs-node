package meta

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// TODO: Place in SDK.
const (
	attrECPartIdx = "__NEOFS__EC_PART_IDX"
)

func (db *DB) ResolveECPartByIdx(cnr cid.ID, parent oid.ID, idx int) (oid.ID, error) {
	// FIXME: check degraded
	var res oid.ID
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, err = db.resolveECPartByIdxTx(tx, cnr, parent, idx)
		return err
	})
	return res, err
}

func (db *DB) resolveECPartByIdxTx(tx *bbolt.Tx, cnr cid.ID, parent oid.ID, idx int) (oid.ID, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}
	metaBktCursor := metaBkt.Cursor()

	pref := slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
		parent[:], objectcore.MetaAttributeDelimiter,
	)
	k, _ := metaBktCursor.Seek(pref)
	partID, ok := bytes.CutPrefix(k, pref)
	if !ok {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}
	if len(partID) != oid.Size {
		return oid.ID{}, invalidMetaBucketKeyErr(k, fmt.Errorf("wrong OID len %d", len(partID)))
	}

	k = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(attrECPartIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(idx)))
	if metaBkt.Get(k) == nil {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}

	return oid.ID(partID), nil
}

func (db *DB) ResolveAnyECPart(cnr cid.ID, parent oid.ID) (oid.ID, error) {
	// FIXME: check degraded
	var res oid.ID
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, err = db.resolveAnyECPartTx(tx, cnr, parent)
		return err
	})
	return res, err
}

func (db *DB) resolveAnyECPartTx(tx *bbolt.Tx, cnr cid.ID, parent oid.ID) (oid.ID, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}
	metaBktCursor := metaBkt.Cursor()

	pref := slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
		parent[:], objectcore.MetaAttributeDelimiter,
	)
	k, _ := metaBktCursor.Seek(pref)
	partID, ok := bytes.CutPrefix(k, pref)
	if !ok {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}
	if len(partID) != oid.Size {
		return oid.ID{}, invalidMetaBucketKeyErr(k, fmt.Errorf("wrong OID len %d", len(partID)))
	}

	pref = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(attrECPartIdx), objectcore.MetaAttributeDelimiter)
	k, _ = metaBktCursor.Seek(pref)
	if !bytes.HasPrefix(k, pref) {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}

	return oid.ID(partID), nil
}
