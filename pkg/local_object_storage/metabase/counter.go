package meta

import (
	"encoding/binary"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

var objectPhyCounterKey = []byte("phy_counter")
var objectLogicCounterKey = []byte("logic_counter")

type objectType uint8

const (
	_ objectType = iota
	phy
	logical
)

// ObjectCounters groups object counter
// according to metabase state.
type ObjectCounters struct {
	logic uint64
	phy   uint64
}

// Logic returns logical object counter.
func (o ObjectCounters) Logic() uint64 {
	return o.logic
}

// Phy returns physical object counter.
func (o ObjectCounters) Phy() uint64 {
	return o.phy
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
		res.phy, res.logic = getCounters(tx)
		return nil
	})

	return res, err
}

func getCounters(tx *bbolt.Tx) (uint64, uint64) {
	var phyC, logicC uint64

	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(objectPhyCounterKey)
		if len(data) == 8 {
			phyC = binary.LittleEndian.Uint64(data)
		}

		data = b.Get(objectLogicCounterKey)
		if len(data) == 8 {
			logicC = binary.LittleEndian.Uint64(data)
		}
	}

	return phyC, logicC
}

// updateCounter updates the object counter. Tx MUST be writable.
// If inc == `true`, increases the counter, decreases otherwise.
func (db *DB) updateCounter(tx *bbolt.Tx, typ objectType, delta uint64, inc bool) error {
	b := tx.Bucket(shardInfoBucket)
	if b == nil {
		return nil
	}

	var counter uint64
	var counterKey []byte

	switch typ {
	case phy:
		counterKey = objectPhyCounterKey
	case logical:
		counterKey = objectLogicCounterKey
	default:
		panic("unknown object type counter")
	}

	data := b.Get(counterKey)
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

	return b.Put(counterKey, newCounter)
}

// syncCounter updates object counters according to metabase state:
// it counts all the physically/logically stored objects using internal
// indexes. Tx MUST be writable.
//
// Does nothing if counters are not empty and force is false. If force is
// true, updates the counters anyway.
func syncCounter(tx *bbolt.Tx, force bool) error {
	b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
	if err != nil {
		return fmt.Errorf("could not get shard info bucket: %w", err)
	}

	if !force && len(b.Get(objectPhyCounterKey)) == 8 && len(b.Get(objectLogicCounterKey)) == 8 {
		// the counters are already inited
		return nil
	}

	var addr oid.Address
	var phyCounter uint64
	var logicCounter uint64

	graveyardBKT := tx.Bucket(graveyardBucketName)
	garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
	garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
	key := make([]byte, addressKeySize)

	err = iteratePhyObjects(tx, func(cnr cid.ID, obj oid.ID) error {
		phyCounter++

		addr.SetContainer(cnr)
		addr.SetObject(obj)

		metaBucket := tx.Bucket(metaBucketKey(cnr))
		var metaCursor *bbolt.Cursor
		if metaBucket != nil {
			metaCursor = metaBucket.Cursor()
		}

		// check if an object is available: not with GCMark
		// and not covered with a tombstone
		if inGraveyardWithKey(metaCursor, addressKey(addr, key), graveyardBKT, garbageObjectsBKT, garbageContainersBKT) == statusAvailable {
			logicCounter++
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not iterate objects: %w", err)
	}

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, phyCounter)

	err = b.Put(objectPhyCounterKey, data)
	if err != nil {
		return fmt.Errorf("could not update phy object counter: %w", err)
	}

	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, logicCounter)

	err = b.Put(objectLogicCounterKey, data)
	if err != nil {
		return fmt.Errorf("could not update logic object counter: %w", err)
	}

	return nil
}
