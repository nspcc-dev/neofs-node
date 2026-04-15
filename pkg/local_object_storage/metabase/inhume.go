package meta

import (
	"bytes"
	"encoding/binary"
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

// MarkGarbage marks objects to be physically removed from shard.
func (db *DB) MarkGarbage(cnr cid.ID, addrs []oid.ID) (ContainerGarbageDiff, error) {
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
		objsInCnr   = make([]oid.ID, 0, len(addrs))
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
		err = markGarbageInContainer(metaCursor, &counterDiff, cnr, objsInCnr, currEpoch)
		if err != nil {
			return fmt.Errorf("marking objects for %s container: %w", cnr, err)
		}

		err := updateCounter(metaBucket, gcCounter, int64(counterDiff.NewGarbage))
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

func markGarbageInContainer(metaCursor *bbolt.Cursor, diff *ContainerGarbageDiff, cnr cid.ID, objs []oid.ID, currEpoch uint64) error {
	var (
		addr       oid.Address
		metaBucket = metaCursor.Bucket()
	)
	addr.SetContainer(cnr)

	for _, id := range objs {
		var garbKey = mkGarbageKey(id)

		k, _ := metaCursor.Seek(garbKey)
		if bytes.Equal(k, garbKey) {
			continue
		}

		addr.SetObject(id)

		obj, err := get(metaCursor, addr, false, true, currEpoch)
		if err == nil {
			if inGarbage(metaCursor, id) == statusAvailable && string(getObjAttribute(metaCursor, id, object.FilterPhysical)) == binPropMarker {
				diff.PayloadDiff -= int64(obj.PayloadSize())
			}
		}
		err = metaBucket.Put(garbKey, nil)
		if err != nil {
			return err
		}
		diff.NewGarbage++
	}

	return nil
}

// InhumeContainer marks every object in a container as removed.
// Any further [DB.Get] calls will return [apistatus.ObjectNotFound]
// errors. Returns number of available objects marked with GC.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
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
		err = resetContainerCounters(metaBkt, counters.Phy)
		if err != nil {
			return fmt.Errorf("reset container counters: %w", err)
		}

		res.Phy = -int(counters.Phy)
		res.Root = -int(counters.Root)
		res.TS = -int(counters.TS)
		res.Lock = -int(counters.Lock)
		res.Link = -int(counters.Link)
		res.GC = int(counters.Phy) - int(counters.GC)
		res.Payload = -int64(counters.Payload)

		return nil
	})

	return res, err
}

func resetContainerCounters(metaBkt *bbolt.Bucket, newGCCounter uint64) error {
	err := metaBkt.Put([]byte{metaPrefixPhyCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset phy counter: %w", err)
	}
	err = metaBkt.Put([]byte{metaPrefixRootCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset root counter: %w", err)
	}
	err = metaBkt.Put([]byte{metaPrefixTSCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset ts counter: %w", err)
	}
	err = metaBkt.Put([]byte{metaPrefixLockCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset lock counter: %w", err)
	}
	err = metaBkt.Put([]byte{metaPrefixLinkCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset link counter: %w", err)
	}
	err = metaBkt.Put([]byte{metaPrefixPayloadCounter}, make([]byte, 8))
	if err != nil {
		return fmt.Errorf("reset payload counter: %w", err)
	}

	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, newGCCounter)
	err = metaBkt.Put([]byte{metaPrefixGCCounter}, raw)
	if err != nil {
		return fmt.Errorf("reset gc counter: %w", err)
	}

	return nil
}
