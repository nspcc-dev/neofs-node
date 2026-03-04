package meta

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// unused starting from 10 DB version.
var objectPhyCounterKey = []byte("phy_counter")
var objectLogicCounterKey = []byte("logic_counter")

// CountersDiff groups counters diff after operation on [DB]. Positive and
// negative values are possible.
type CountersDiff struct {
	Phy  int
	Root int
	TS   int
	Lock int
	Link int
	GC   int
}

type objectType uint8

const (
	_ objectType = iota
	phyCounter
	logicalCounter // removed in 10 metabase version
	rootCounter
	tsCounter
	lockCounter
	linkCounter
	gcCounter
)

// ObjectCounters groups object counters
// according to metabase state.
type ObjectCounters struct {
	Phy  uint64
	Root uint64
	TS   uint64
	Lock uint64
	Link uint64
	GC   uint64
}

// ObjectCounters returns object counters that metabase has
// tracked since it was opened and initialized.
//
// Returns only the errors that do not allow reading counter
// in Bolt database.
func (db *DB) ObjectCounters() (ObjectCounters, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ObjectCounters{}, ErrDegradedMode
	}

	var res ObjectCounters
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		var err error
		res, err = getCounters(tx)
		return err
	})

	return res, err
}

func getCounters(tx *bbolt.Tx) (ObjectCounters, error) {
	fetchCounter := func(b *bbolt.Bucket, prefix byte) uint64 {
		data := b.Get([]byte{prefix})
		if len(data) != 8 {
			return 0
		}
		return binary.LittleEndian.Uint64(data)
	}

	var res ObjectCounters
	return res, tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		if name[0] != metadataPrefix {
			return nil
		}
		res.Phy += fetchCounter(b, metaPrefixPhyCounter)
		res.Root += fetchCounter(b, metaPrefixRootCounter)
		res.TS += fetchCounter(b, metaPrefixTSCounter)
		res.Lock += fetchCounter(b, metaPrefixLockCounter)
		res.Link += fetchCounter(b, metaPrefixLinkCounter)

		if containerMarkedGC(b.Cursor()) {
			res.GC += res.Phy
			return nil
		}
		res.GC += fetchCounter(b, metaPrefixGCCounter)

		return nil
	})
}

// updateCounter updates the object counter. Tx MUST be writable.
// If inc == `true`, increases the counter, decreases otherwise.
func updateCounter(metaBkt *bbolt.Bucket, typ objectType, delta uint64, inc bool) error {
	var (
		counter    uint64
		counterKey = make([]byte, 1)
	)

	switch typ {
	case phyCounter:
		counterKey[0] = metaPrefixPhyCounter
	case rootCounter:
		counterKey[0] = metaPrefixRootCounter
	case tsCounter:
		counterKey[0] = metaPrefixTSCounter
	case lockCounter:
		counterKey[0] = metaPrefixLockCounter
	case linkCounter:
		counterKey[0] = metaPrefixLinkCounter
	case gcCounter:
		counterKey[0] = metaPrefixGCCounter
	default:
		panic("unknown object type counter")
	}

	data := metaBkt.Get(counterKey)
	if len(data) == 8 {
		counter = binary.LittleEndian.Uint64(data)
	}

	if inc {
		counter += delta
	} else if counter <= delta {
		counter = 0
	} else {
		counter -= delta
	}

	newCounter := make([]byte, 8)
	binary.LittleEndian.PutUint64(newCounter, counter)

	return metaBkt.Put(counterKey, newCounter)
}

// syncCounter updates object counters according to metabase state:
// it counts all the physically/logically stored objects using internal
// indexes. Tx MUST be writable.
//
// Does nothing if counters are not empty and force is false. If force is
// true, updates the counters anyway.
func syncCounter(tx *bbolt.Tx, force bool) error {
	return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		if name[0] != metadataPrefix {
			return nil
		}

		err := syncContainerCounters(b, force)
		if err != nil {
			cnr, err := cid.DecodeBytes(name[1:])
			if err != nil {
				return fmt.Errorf("sync container counters: %w", err)
			}
			return fmt.Errorf("sync %s container counters: %w", cnr, err)
		}

		return nil
	})
}

func syncContainerCounters(b *bbolt.Bucket, force bool) error {
	if !force &&
		len(b.Get([]byte{metaPrefixPhyCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixRootCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixTSCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixLockCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixLinkCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixGCCounter})) == 8 {
		// the counters are already inited
		return nil
	}

	var (
		phyCounter  uint64
		rootCounter uint64
		tsCounter   uint64
		lockCounter uint64
		linkCounter uint64
		gcCounter   uint64
	)

	c := b.Cursor()
	for obj := range iterAttrVal(b.Cursor(), object.FilterType, []byte(object.TypeRegular.String())) {
		if string(getObjAttribute(c, obj, object.FilterPhysical)) == binPropMarker {
			phyCounter++
		}
		if string(getObjAttribute(c, obj, object.FilterRoot)) == binPropMarker {
			rootCounter++
		}
		if inGarbage(c, obj) != statusAvailable {
			gcCounter++
		}
	}

	countNonRegularObjects := func(typ object.Type, counter *uint64) {
		for obj := range iterAttrVal(b.Cursor(), object.FilterType, []byte(typ.String())) {
			phyCounter++
			*counter++
			if inGarbage(c, obj) != statusAvailable {
				gcCounter++
				continue
			}
		}
	}
	countNonRegularObjects(object.TypeTombstone, &tsCounter)
	countNonRegularObjects(object.TypeLock, &lockCounter)
	countNonRegularObjects(object.TypeLink, &linkCounter)

	putCounter := func(counterKeyPref byte, counter uint64) error {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, counter)
		err := b.Put([]byte{counterKeyPref}, data)
		if err != nil {
			return err
		}

		return nil
	}
	err := putCounter(metaPrefixPhyCounter, phyCounter)
	if err != nil {
		return fmt.Errorf("sync PHY counter: %w", err)
	}
	err = putCounter(metaPrefixRootCounter, rootCounter)
	if err != nil {
		return fmt.Errorf("sync ROOT counter: %w", err)
	}
	err = putCounter(metaPrefixTSCounter, tsCounter)
	if err != nil {
		return fmt.Errorf("sync TS counter: %w", err)
	}
	err = putCounter(metaPrefixLockCounter, lockCounter)
	if err != nil {
		return fmt.Errorf("sync LOCK counter: %w", err)
	}
	err = putCounter(metaPrefixLinkCounter, linkCounter)
	if err != nil {
		return fmt.Errorf("sync LINK counter: %w", err)
	}
	err = putCounter(metaPrefixGCCounter, gcCounter)
	if err != nil {
		return fmt.Errorf("sync GC counter: %w", err)
	}

	return nil
}
