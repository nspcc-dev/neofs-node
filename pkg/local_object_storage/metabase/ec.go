package meta

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ResolveECPart resolves object that carries EC part produced within cnr for
// parent object and indexed by pi, checks its availability and returns its ID.
//
// If DB is disabled by mode (e.g. [DB.SetMode]), ResolveECPart returns
// [ErrDegradedMode].
//
// If object has expired, ResolveECPart returns [ErrObjectIsExpired].
//
// If object exists but tombstoned (via [DB.Inhume] or stored tombstone object),
// ResolveECPart returns [apistatus.ErrObjectAlreadyRemoved].
//
// If object is marked as garbage (via [DB.MarkGarbage]), ResolveECPart returns
// [apistatus.ErrObjectNotFound].
//
// If object is locked (via [DB.Lock] or stored locker object), ResolveECPart
// ignores expiration, tombstone and garbage marks.
func (db *DB) ResolveECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()
	if db.mode.NoMetabase() {
		return oid.ID{}, ErrDegradedMode
	}

	var res oid.ID
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, err = db.resolveECPartTx(tx, cnr, parent, pi)
		return err
	})
	return res, err
}

func (db *DB) resolveECPartTx(tx *bbolt.Tx, cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	metaBkt := tx.Bucket(metaBucketKey(cnr))
	if metaBkt == nil {
		return oid.ID{}, apistatus.ErrObjectNotFound
	}

	metaBktCursor := metaBkt.Cursor()

	id, err := db.resolveECPartInMetaBucket(metaBktCursor, parent, pi)
	if err != nil {
		return oid.ID{}, err
	}

	switch objectStatus(tx, metaBktCursor, oid.NewAddress(cnr, id), db.epochState.CurrentEpoch()) {
	case statusGCMarked:
		return oid.ID{}, apistatus.ErrObjectNotFound
	case statusTombstoned:
		return oid.ID{}, apistatus.ErrObjectAlreadyRemoved
	case statusExpired:
		return oid.ID{}, ErrObjectIsExpired
	}

	return id, nil
}

func (db *DB) resolveECPartInMetaBucket(crs *bbolt.Cursor, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	pref := slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(object.FilterParentID), objectcore.MetaAttributeDelimiter,
		parent[:], objectcore.MetaAttributeDelimiter,
	)

	var partCrs *bbolt.Cursor
	var rulePref, partPref []byte
	for k, _ := crs.Seek(pref); ; k, _ = crs.Next() {
		partID, ok := bytes.CutPrefix(k, pref)
		if !ok {
			return oid.ID{}, apistatus.ErrObjectNotFound
		}
		if len(partID) != oid.Size {
			return oid.ID{}, invalidMetaBucketKeyErr(k, fmt.Errorf("wrong OID len %d", len(partID)))
		}
		if islices.AllZeros(partID) {
			return oid.ID{}, invalidMetaBucketKeyErr(k, oid.ErrZero)
		}

		if partCrs == nil {
			partCrs = crs.Bucket().Cursor()
		}

		if rulePref == nil {
			// TODO: make and reuse one buffer for all keys
			rulePref = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(iec.AttributeRuleIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.RuleIndex)))
		} else {
			copy(rulePref[1:], partID)
		}
		if k, _ = partCrs.Seek(rulePref); !bytes.Equal(k, rulePref) { // Cursor.Seek is more lightweight than Bucket.Get making cursor inside
			continue
		}

		if partPref == nil {
			partPref = slices.Concat([]byte{metaPrefixIDAttr}, partID, []byte(iec.AttributePartIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.Index)))
		} else {
			copy(partPref[1:], partID)
		}
		if k, _ = partCrs.Seek(partPref); bytes.Equal(k, partPref) {
			return oid.ID(partID), nil
		}
	}
}
