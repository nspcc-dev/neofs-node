package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ResolveECPart resolves object that carries EC part produced within cnr for
// parent object and indexed by pi, checks its availability and returns its ID.
//
// If the object is not EC part but of [object.TypeTombstone], [object.TypeLock]
// or [object.TypeLink] type, ResolveECPart returns its ID instead.
//
// If the object is not EC part but it is size-split, ResolveECPart returns
// [*object.SplitInfoError] with [object.SplitInfo] depending on stored
// relatives. If DB contains the linker, its ID is attached. If DB contains only
// the last child, its ID is attached. If DB contains both objects, linker ID is
// always attached while last child's ID may or may not be attached depending on
// object order.
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

		metaBkt := tx.Bucket(metaBucketKey(cnr))
		if metaBkt == nil {
			return apistatus.ErrObjectNotFound
		}

		res, err = db.resolveECPartInMetaBucket(metaBkt.Cursor(), parent, pi)
		return err
	})
	return res, err
}

// ResolveECPartWithPayloadLen is like [DB.ResolveECPart] but also returns
// payload length.
func (db *DB) ResolveECPartWithPayloadLen(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (oid.ID, uint64, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()
	if db.mode.NoMetabase() {
		return oid.ID{}, 0, ErrDegradedMode
	}

	var id oid.ID
	var ln uint64

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error

		metaBkt := tx.Bucket(metaBucketKey(cnr))
		if metaBkt == nil {
			return apistatus.ErrObjectNotFound
		}

		id, ln, err = db.resolveECPartWithPayloadLen(metaBkt.Cursor(), parent, pi)
		return err
	})

	return id, ln, err
}

func (db *DB) resolveECPartWithPayloadLen(metaBktCrs *bbolt.Cursor, parent oid.ID, pi iec.PartInfo) (oid.ID, uint64, error) {
	id, err := db.resolveECPartInMetaBucket(metaBktCrs, parent, pi)
	if err != nil {
		return oid.ID{}, 0, err
	}

	lnAttr := getObjAttribute(metaBktCrs, id, object.FilterPayloadSize)
	if lnAttr == nil {
		return oid.ID{}, 0, fmt.Errorf("missing index for payload len attribute of object %w", ierrors.ObjectID(id))
	}

	ln, err := strconv.ParseUint(string(lnAttr), 10, 64)
	if err != nil {
		var ne *strconv.NumError
		if !errors.As(err, &ne) { // must never happen
			return oid.ID{}, 0, fmt.Errorf("invalid payload len attribute of object %w", ierrors.ObjectID(id))
		}
		return oid.ID{}, 0, fmt.Errorf("invalid payload len attribute of object %w: parse to uint64: %w", ierrors.ObjectID(id), ne.Err)
	}

	return id, ln, nil
}

func (db *DB) resolveECPartInMetaBucket(crs *bbolt.Cursor, parent oid.ID, pi iec.PartInfo) (oid.ID, error) {
	metaBkt := crs.Bucket()

	switch objectStatus(crs, parent, db.epochState.CurrentEpoch()) {
	case statusGCMarked:
		return oid.ID{}, apistatus.ErrObjectNotFound
	case statusTombstoned:
		return oid.ID{}, apistatus.ErrObjectAlreadyRemoved
	case statusExpired:
		return oid.ID{}, ErrObjectIsExpired
	}

	var partCrs *bbolt.Cursor
	var rulePref, partPref, typePref []byte
	var sizeSplitInfo *object.SplitInfo
	isParent := false
	for id := range iterAttrVal(crs, object.FilterParentID, parent[:]) {
		if id.IsZero() {
			return oid.ID{}, fmt.Errorf("invalid child of %s parent: %w", parent, oid.ErrZero)
		}

		isParent = true

		if partCrs == nil {
			partCrs = metaBkt.Cursor()
		}

		if rulePref == nil {
			// TODO: make and reuse one buffer for all keys
			rulePref = slices.Concat([]byte{metaPrefixIDAttr}, id[:], []byte(iec.AttributeRuleIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.RuleIndex)))
		} else {
			copy(rulePref[1:], id[:])
		}
		k, _ := partCrs.Seek(rulePref)
		if !bytes.Equal(k, rulePref) { // Cursor.Seek is more lightweight than Bucket.Get making cursor inside
			if typePref == nil {
				typePref = make([]byte, metaIDTypePrefixSize)
				fillIDTypePrefix(typePref)
			}
			if typ, err := fetchTypeForIDWBuf(partCrs, typePref, id); err == nil && typ == object.TypeLink {
				if sizeSplitInfo == nil {
					sizeSplitInfo = new(object.SplitInfo)
				}

				sizeSplitInfo.SetLink(id)

				return oid.ID{}, object.NewSplitInfoError(sizeSplitInfo)
			}

			if (sizeSplitInfo == nil || sizeSplitInfo.GetLastPart().IsZero()) && getObjAttribute(partCrs, id, object.FilterFirstSplitObject) != nil {
				if sizeSplitInfo == nil {
					sizeSplitInfo = new(object.SplitInfo)
				}

				sizeSplitInfo.SetLastPart(id)
				// continue because next item may be a linker. If so, it's the most informative
			}

			continue
		}

		if partPref == nil {
			partPref = slices.Concat([]byte{metaPrefixIDAttr}, id[:], []byte(iec.AttributePartIdx), objectcore.MetaAttributeDelimiter, []byte(strconv.Itoa(pi.Index)))
		} else {
			copy(partPref[1:], id[:])
		}
		if k, _ = partCrs.Seek(partPref); bytes.Equal(k, partPref) {
			return id, nil
		}
	}
	if !isParent { // neither tombstone nor lock can be a parent
		if typePref == nil {
			typePref = make([]byte, metaIDTypePrefixSize)
			fillIDTypePrefix(typePref)
		}
		if typ, err := fetchTypeForIDWBuf(crs, typePref, parent); err == nil && (typ == object.TypeTombstone || typ == object.TypeLock || typ == object.TypeLink) {
			return parent, nil
		}
	}

	if sizeSplitInfo != nil {
		return oid.ID{}, object.NewSplitInfoError(sizeSplitInfo)
	}

	return oid.ID{}, apistatus.ErrObjectNotFound
}
