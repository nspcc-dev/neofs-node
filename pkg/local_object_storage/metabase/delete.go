package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// RemovedObjects describes single item handled by [DB.Delete].
type RemovedObject struct {
	ID         oid.ID
	PayloadLen uint64
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
	// Actually removed objects. First len(addrs) elements always contain addrs
	// passed to [DB.Delete], but order is different in general.
	RemovedObjects []RemovedObject
	// CountersDiff describes counters changes after [DB.Delete] operation.
	Counters CountersDiff
}

// Delete removes object records from metabase indexes.
// Does not stop on an error if there are more objects to handle requested;
// returns the first error appeared with a number of deleted objects wrapped.
//
// Delete also looks up for objects that are hardly linked with elements of
// addrs list but not in the list themselves. If there are any, they are also
// deleted.
func (db *DB) Delete(cnr cid.ID, addrs []oid.ID) (DeleteRes, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return DeleteRes{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return DeleteRes{}, ErrReadOnlyMode
	}

	var diff CountersDiff
	var err error
	var removed []RemovedObject

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		var metaBucket = tx.Bucket(metaBucketKey(cnr))
		if metaBucket == nil {
			return nil
		}
		var metaCursor = metaBucket.Cursor()
		// We need to clear slice because tx can try to execute multiple times.
		diff, removed, err = db.deleteGroup(metaCursor, cnr, addrs)
		return err
	})
	if err == nil {
		for i := range addrs {
			storagelog.Write(db.log,
				storagelog.AddressField(oid.NewAddress(cnr, addrs[i])),
				storagelog.OpField("metabase DELETE"))
		}
	}
	return DeleteRes{
		Counters:       diff,
		RemovedObjects: removed,
	}, err
}

// deleteGroup deletes object from the metabase. Handles removal of the
// references of the split objects.
// The first return value is a physical objects removed number: physical
// objects that were stored. The second return value is a logical objects
// removed number: objects that were available (without Tombstones, GCMarks
// non-expired, etc.)
func (db *DB) deleteGroup(metaCursor *bbolt.Cursor, cnr cid.ID, addrs []oid.ID) (CountersDiff, []RemovedObject, error) {
	var errorCount int
	var firstErr error
	var diff CountersDiff

	removedObjs, err := supplementRemovedObjects(metaCursor, addrs)
	if err != nil {
		return diff, nil, fmt.Errorf("extend removed objects: %w", err)
	}

	for i := range removedObjs {
		objectDiff, err := db.delete(metaCursor, cnr, removedObjs[i].ID)
		if err != nil {
			errorCount++
			var addr = oid.NewAddress(cnr, removedObjs[i].ID)
			db.log.Warn("failed to delete object", zap.Stringer("addr", addr), zap.Error(err))
			if firstErr == nil {
				firstErr = fmt.Errorf("%s object delete fail: %w", addr, err)
			}

			continue
		}

		diff.add(objectDiff)
		removedObjs[i].PayloadLen = uint64(-objectDiff.Payload)
	}

	if firstErr != nil {
		all := len(removedObjs)
		success := all - errorCount
		return diff, nil, fmt.Errorf("deleted %d out of %d objects, first error: %w", success, all, firstErr)
	}

	return diff, removedObjs, nil
}

// delete removes object indexes from the metabase.
func (db *DB) delete(metaCursor *bbolt.Cursor, cnr cid.ID, addr oid.ID) (CountersDiff, error) {
	diff, err := deleteMetadata(metaCursor, db.log, cnr, addr, false)
	if err != nil {
		if !errors.Is(err, errNonPhy) {
			return CountersDiff{}, fmt.Errorf("can't remove metadata indexes: %w", err)
		}
	}

	return diff, nil
}

// forms list of objects from addrs and their missing parts.
// [RemovedObject.PayloadLen] is not initialized.
func supplementRemovedObjects(cur *bbolt.Cursor, addrs []oid.ID) ([]RemovedObject, error) {
	var (
		err error
		res = make([]RemovedObject, len(addrs))
	)
	for i := range addrs {
		res[i].ID = addrs[i]
	}
	for i := range addrs {
		res, err = supplementRemovedECParts(res, cur, addrs, addrs[i])
		if err != nil {
			return nil, fmt.Errorf("collect EC parts for %s: %w", addrs[i], err)
		}
	}

	return res, nil
}

// extends res with EC parts of addr which are not in addrs and returns updated res.
func supplementRemovedECParts(res []RemovedObject, cnrMetaCrs *bbolt.Cursor, addrs []oid.ID, parent oid.ID) ([]RemovedObject, error) {
	var partCrs *bbolt.Cursor
	var ecPref []byte
	for id := range iterAttrVal(cnrMetaCrs, object.FilterParentID, parent[:]) {
		if id.IsZero() {
			return nil, fmt.Errorf("invalid child of %s parent: %w", parent, oid.ErrZero)
		}

		if partCrs == nil {
			partCrs = cnrMetaCrs.Bucket().Cursor()
		}

		if ecPref == nil {
			ecPref = slices.Concat([]byte{metaPrefixIDAttr}, id[:], []byte(iec.AttributePrefix)) // any of EC attributes
		} else {
			copy(ecPref[1:], id[:])
		}

		k, _ := partCrs.Seek(ecPref)
		if !bytes.HasPrefix(k, ecPref) {
			continue
		}

		if !slices.Contains(addrs, id) {
			res = append(res, RemovedObject{
				ID: id,
			})
		}
	}

	return res, nil
}
