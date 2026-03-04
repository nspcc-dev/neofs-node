package meta

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = logicerr.New("lock object removal")

// MarkGarbage marks objects to be physically removed from shard.
func (db *DB) MarkGarbage(addrs ...oid.Address) (int, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, ErrReadOnlyMode
	}

	var (
		currEpoch      = db.epochState.CurrentEpoch()
		err            error
		actuallyMarked int
		objsInCnr      []oid.ID
	)
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		// collect children
		// TODO: Do not extend addrs, do in the main loop. This likely would be more efficient regarding memory.
		for i := range addrs {
			objsInCnr = objsInCnr[:0]

			cnr := addrs[i].Container()
			if slices.ContainsFunc(addrs[:i], func(a oid.Address) bool { return a.Container() == cnr }) {
				continue // already handled, see loop below
			}

			metaBucket := tx.Bucket(metaBucketKey(cnr))
			if metaBucket == nil {
				continue
			}
			metaCursor := metaBucket.Cursor()

			for j := range addrs[i:] {
				if j != 0 && addrs[i+j].Container() != cnr {
					continue
				}
				parObj := addrs[i+j].Object()
				partIDs, err := collectChildren(metaCursor, cnr, parObj)
				if err != nil {
					return fmt.Errorf("collect EC parts: %w", err)
				}
				objsInCnr = append(objsInCnr, parObj)
				objsInCnr = append(objsInCnr, partIDs...)
			}

			err = markGarbageInContainer(metaCursor, cnr, objsInCnr, currEpoch)
			if err != nil {
				return fmt.Errorf("marking objects for %s container: %w", cnr, err)
			}

			actuallyMarked += len(objsInCnr)
			err := updateCounter(metaBucket, gcCounter, len(objsInCnr))
			if err != nil {
				return fmt.Errorf("update gc counter to %d: %w", len(objsInCnr), err)
			}
		}

		return nil
	})

	return actuallyMarked, err
}

func markGarbageInContainer(metaCursor *bbolt.Cursor, cnr cid.ID, objs []oid.ID, currEpoch uint64) error {
	var (
		addr       oid.Address
		metaBucket = metaCursor.Bucket()
	)
	addr.SetContainer(cnr)

	for _, id := range objs {
		addr.SetObject(id)

		obj, err := get(metaCursor, addr, false, true, currEpoch)
		if err == nil {
			// if object is stored, and it is regular object then update bucket
			// with container size estimations
			if obj.Type() == object.TypeRegular {
				err := changeContainerInfo(metaBucket.Tx(), cnr, -int(obj.PayloadSize()), -1)
				if err != nil {
					return err
				}
			}
		}

		err = metaBucket.Put(mkGarbageKey(id), nil)
		if err != nil {
			return err
		}
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

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
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

		return resetContainerSize(tx, cID)
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

	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, newGCCounter)
	err = metaBkt.Put([]byte{metaPrefixGCCounter}, raw)
	if err != nil {
		return fmt.Errorf("reset gc counter: %w", err)
	}

	return nil
}
