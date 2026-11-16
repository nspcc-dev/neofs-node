package meta

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = logicerr.New("lock object removal")

// ContainerGarbageDiff groups [DB.MarkGarbage] operation counters changes by a container.
type ContainerGarbageDiff struct {
	NewGarbage  int
	PayloadDiff int64
}

// GarbageMark specifies why an object is marked for garbage collection.
type GarbageMark byte

const (
	GarbageMarkDefault GarbageMark = iota
	GarbageMarkRedundant
)

var redundantGarbageMark = []byte{byte(GarbageMarkRedundant)}

func (m GarbageMark) valid() bool {
	switch m {
	case GarbageMarkDefault, GarbageMarkRedundant:
		return true
	default:
		return false
	}
}

// MarkGarbage marks objects to be physically removed from shard. Redundant
// objects remain readable until they are physically removed by GC.
func (db *DB) MarkGarbage(cnr cid.ID, addrs []oid.ID, mark GarbageMark) (ContainerGarbageDiff, error) {
	if !mark.valid() {
		return ContainerGarbageDiff{}, fmt.Errorf("invalid garbage mark %d", mark)
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ContainerGarbageDiff{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ContainerGarbageDiff{}, ErrReadOnlyMode
	}

	var (
		currEpoch   = db.epochState.CurrentEpoch()
		err         error
		counterDiff ContainerGarbageDiff
	)
	err = db.boltDB.Batch(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket(metaBucketKey(cnr))
		if metaBucket == nil {
			return nil
		}
		metaCursor := metaBucket.Cursor()
		if containerMarkedGC(metaCursor) {
			return nil
		}
		var objsInCnr = make([]oid.ID, 0, len(addrs))

		// collect children
		// TODO: Do not extend addrs, do in the main loop. This likely would be more efficient regarding memory.
		for i := range addrs {
			parObj := addrs[i]
			partIDs, err := collectChildren(metaCursor, cnr, parObj)
			if err != nil {
				return fmt.Errorf("collect EC parts: %w", err)
			}
			objsInCnr = append(objsInCnr, parObj)
			objsInCnr = append(objsInCnr, partIDs...)
		}
		counterDiff, err = markGarbageInContainer(metaCursor, cnr, objsInCnr, currEpoch, mark)
		if err != nil {
			return fmt.Errorf("marking objects for %s container: %w", cnr, err)
		}

		err = updateCounter(metaBucket, gcCounter, int64(counterDiff.NewGarbage))
		if err != nil {
			return fmt.Errorf("update %s container's gc counter to %d: %w", cnr, counterDiff.NewGarbage, err)
		}
		err = updateCounter(metaBucket, payloadCounter, counterDiff.PayloadDiff)
		if err != nil {
			return fmt.Errorf("update %s container's user payload counter to %d: %w", cnr, counterDiff.PayloadDiff, err)
		}

		return nil
	})

	return counterDiff, err
}

func markGarbageInContainer(metaCursor *bbolt.Cursor, cnr cid.ID, objs []oid.ID, currEpoch uint64, mark GarbageMark) (ContainerGarbageDiff, error) {
	var (
		addr       oid.Address
		diff       ContainerGarbageDiff
		metaBucket = metaCursor.Bucket()
	)
	addr.SetContainer(cnr)

	for _, id := range objs {
		var garbKey = mkGarbageKey(id)

		k, v := metaCursor.Seek(garbKey)
		if bytes.Equal(k, garbKey) {
			if mark == GarbageMarkDefault && len(v) > 0 {
				if err := metaBucket.Put(garbKey, nil); err != nil {
					return diff, err
				}
			}
			continue
		}

		addr.SetObject(id)

		obj, err := get(metaCursor, addr, false, true, currEpoch, 0)
		if err == nil {
			if inGarbage(metaCursor, id) == statusAvailable && string(getObjAttribute(metaCursor, id, object.FilterPhysical)) == binPropMarker {
				diff.PayloadDiff -= int64(obj.PayloadSize())
			}
		}
		var markValue []byte
		if mark != GarbageMarkDefault {
			markValue = []byte{byte(mark)}
		}
		err = metaBucket.Put(garbKey, markValue)
		if err != nil {
			return diff, err
		}
		diff.NewGarbage++
	}

	return diff, nil
}

// InhumeContainer marks every object in a container as removed.
// Any further [DB.Get] calls will return [apistatus.ObjectNotFound]
// errors.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
//
// The objects stay in the DB until GC deletes them, see [DB.GetGarbage] and
// [DB.Delete], and so does their count by PHY and by type. All of them
// become garbage and none of their payload belongs to the container any
// longer: InhumeContainer returns this change of the GC and the payload
// counters, the only ones it makes.
func (db *DB) InhumeContainer(cID cid.ID) (CountersDiff, error) {
	var res CountersDiff

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return res, ErrReadOnlyMode
	}

	err := db.boltDB.Batch(func(tx *bbolt.Tx) error {
		metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
		if err != nil {
			return fmt.Errorf("create meta bucket: %w", err)
		}
		if err := metaBkt.Put(containerGCMarkKey, nil); err != nil {
			return fmt.Errorf("write container GC mark: %w", err)
		}

		counters := getCountersByContainer(metaBkt)
		// Batch can call this function more than once.
		res = CountersDiff{
			GC:      int(counters.Phy) - int(counters.GC),
			Payload: -int64(counters.Payload),
		}

		err = applyDiff(metaBkt, res)
		if err != nil {
			return fmt.Errorf("update container counters: %w", err)
		}

		return nil
	})

	return res, err
}
