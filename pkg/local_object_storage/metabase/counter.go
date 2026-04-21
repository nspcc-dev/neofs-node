package meta

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// unused starting from 10 DB version.
var objectPhyCounterKey = []byte("phy_counter")
var objectLogicCounterKey = []byte("logic_counter")

// CountersDiff groups counters diff after operation on [DB]. Positive and
// negative values are possible.
type CountersDiff struct {
	Phy     int
	Root    int
	TS      int
	Lock    int
	Link    int
	GC      int
	Payload int64
}

func (c *CountersDiff) add(c2 CountersDiff) {
	c.Phy += c2.Phy
	c.Root += c2.Root
	c.TS += c2.TS
	c.Lock += c2.Lock
	c.Link += c2.Link
	c.GC += c2.GC
	c.Payload += c2.Payload
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
	payloadCounter
)

// ObjectCounters groups object counters
// according to metabase state.
type ObjectCounters struct {
	Phy     uint64
	Root    uint64
	TS      uint64
	Lock    uint64
	Link    uint64
	GC      uint64
	Payload uint64
}

func (o *ObjectCounters) add(o2 ObjectCounters) {
	o.Phy += o2.Phy
	o.Root += o2.Root
	o.TS += o2.TS
	o.Lock += o2.Lock
	o.Link += o2.Link
	o.GC += o2.GC
	o.Payload += o2.Payload
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

func applyDiff(metaBkt *bbolt.Bucket, diff CountersDiff) error {
	if diff.Phy != 0 {
		err := updateCounter(metaBkt, phyCounter, int64(diff.Phy))
		if err != nil {
			return fmt.Errorf("updating phy counter: %w", err)
		}
	}
	if diff.Root != 0 {
		err := updateCounter(metaBkt, rootCounter, int64(diff.Root))
		if err != nil {
			return fmt.Errorf("updating root counter: %w", err)
		}
	}
	if diff.TS != 0 {
		err := updateCounter(metaBkt, tsCounter, int64(diff.TS))
		if err != nil {
			return fmt.Errorf("updating ts counter: %w", err)
		}
	}
	if diff.Lock != 0 {
		err := updateCounter(metaBkt, lockCounter, int64(diff.Lock))
		if err != nil {
			return fmt.Errorf("updating lock counter: %w", err)
		}
	}
	if diff.Link != 0 {
		err := updateCounter(metaBkt, linkCounter, int64(diff.Link))
		if err != nil {
			return fmt.Errorf("updating link counter: %w", err)
		}
	}
	if diff.GC != 0 {
		err := updateCounter(metaBkt, gcCounter, int64(diff.GC))
		if err != nil {
			return fmt.Errorf("updating gc counter: %w", err)
		}
	}
	if diff.Payload != 0 {
		err := updateCounter(metaBkt, payloadCounter, diff.Payload)
		if err != nil {
			return fmt.Errorf("updating user payload counter: %w", err)
		}
	}

	return nil
}

func getCounters(tx *bbolt.Tx) (ObjectCounters, error) {
	var res ObjectCounters
	return res, tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		if name[0] != metadataPrefix {
			return nil
		}

		res.add(getCountersByContainer(b))

		return nil
	})
}

func getCountersByContainer(metaBucket *bbolt.Bucket) ObjectCounters {
	var (
		res          ObjectCounters
		fetchCounter = func(b *bbolt.Bucket, prefix byte) uint64 {
			data := b.Get([]byte{prefix})
			if len(data) != 8 {
				return 0
			}
			return binary.LittleEndian.Uint64(data)
		}
	)

	res.Phy = fetchCounter(metaBucket, metaPrefixPhyCounter)
	res.Root = fetchCounter(metaBucket, metaPrefixRootCounter)
	res.TS = fetchCounter(metaBucket, metaPrefixTSCounter)
	res.Lock = fetchCounter(metaBucket, metaPrefixLockCounter)
	res.Link = fetchCounter(metaBucket, metaPrefixLinkCounter)
	res.GC = fetchCounter(metaBucket, metaPrefixGCCounter)
	res.Payload = fetchCounter(metaBucket, metaPrefixPayloadCounter)

	return res
}

// updateCounter updates the object counter. Tx MUST be writable.
func updateCounter(metaBkt *bbolt.Bucket, typ objectType, delta int64) error {
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
	case payloadCounter:
		counterKey[0] = metaPrefixPayloadCounter
	default:
		panic(fmt.Sprintf("unknown object type counter: %d", typ))
	}

	data := metaBkt.Get(counterKey)
	if len(data) == 8 {
		counter = binary.LittleEndian.Uint64(data)
	}

	if delta >= 0 {
		counter += uint64(delta)
	} else {
		counter -= min(counter, uint64(-delta))
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
			cnr, decErr := cid.DecodeBytes(name[1:])
			if decErr != nil {
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
		len(b.Get([]byte{metaPrefixGCCounter})) == 8 &&
		len(b.Get([]byte{metaPrefixPayloadCounter})) == 8 {
		// the counters are already inited
		return nil
	}

	var (
		phyCounter          uint64
		rootCounter         uint64
		tsCounter           uint64
		lockCounter         uint64
		linkCounter         uint64
		gcCounter           uint64
		usersPayloadCounter uint64
	)

	c := b.Cursor()
	deadContainer := containerMarkedGC(c)
	cInt := b.Cursor()
	for obj := range iterAttrVal(c, object.FilterPhysical, []byte(binPropMarker)) {
		phyCounter++
		if deadContainer {
			continue
		}
		if inGarbage(cInt, obj) != statusAvailable {
			continue
		}
		sizeRaw := getObjAttribute(cInt, obj, object.FilterPayloadSize)
		size, _ := strconv.ParseUint(string(sizeRaw), 10, 64)
		usersPayloadCounter += size
	}
	if deadContainer {
		err := resetContainerCounters(b, phyCounter)
		if err != nil {
			return fmt.Errorf("reset container counters: %w", err)
		}

		return nil
	}

	for range iterAttrVal(c, object.FilterRoot, []byte(binPropMarker)) {
		rootCounter++
	}
	for range iterPrefixedIDs(c, []byte{metaPrefixGarbage}, oid.ID{}) {
		gcCounter++
	}
	for range iterAttrVal(c, object.FilterType, []byte(object.TypeTombstone.String())) {
		tsCounter++
	}
	for range iterAttrVal(c, object.FilterType, []byte(object.TypeLock.String())) {
		lockCounter++
	}
	for range iterAttrVal(c, object.FilterType, []byte(object.TypeLink.String())) {
		linkCounter++
	}

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
	err = putCounter(metaPrefixPayloadCounter, usersPayloadCounter)
	if err != nil {
		return fmt.Errorf("sync user payload counter: %w", err)
	}

	return nil
}
