package object

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const (
	currentVersion = 8 // it is also a number of fields
)

const (
	networkMagicKey = "network"
	cidKey          = "cid"
	oidKey          = "oid"
	firstPartKey    = "firstPart"
	sizeKey         = "size"
	deletedKey      = "deleted"
	lockedKey       = "locked"
	validUntilKey   = "validUntil"
)

// EncodeReplicationMetaInfo uses NEO's map (strict order) serialized format as a raw
// representation of object's meta information.
//
// This (ordered) format is used (keys are strings):
//
//	"network": network magic
//	"cid": _raw_ container ID (32 bytes)
//	"oid": _raw_ object ID (32 bytes)
//	"firstPart": _raw_ object ID (32 bytes)
//	"size": payload size
//	"deleted": array of _raw_ object IDs
//	"locked": array of _raw_ object IDs
//	"validUntil": last valid block number for meta information
//
// Last valid epoch is object's creation epoch + 10.
func EncodeReplicationMetaInfo(o object.Object, vub uint64, magicNumber uint32) ([]byte, error) {
	firstObj, _ := o.FirstID()
	if firstObj.IsZero() && o.HasParent() && o.SplitID() == nil {
		// object itself is the first one
		firstObj = o.GetID()
	}

	var deleted []oid.ID
	var locked []oid.ID
	switch o.Type() {
	case object.TypeTombstone:
		var t object.Tombstone
		err := t.Unmarshal(o.Payload())
		if err != nil {
			return nil, fmt.Errorf("reading tombstoned objects: %w", err)
		}

		deleted = t.Members()
	case object.TypeLock:
		var l object.Lock
		err := l.Unmarshal(o.Payload())
		if err != nil {
			return nil, fmt.Errorf("reading locked objects: %w", err)
		}

		locked = make([]oid.ID, l.NumberOfMembers())
		l.ReadMembers(locked)
	default:
	}

	cID := o.GetContainerID()
	oID := o.GetID()
	size := o.PayloadSize()

	kvs := []stackitem.MapElement{
		kv(networkMagicKey, magicNumber),
		kv(cidKey, cID[:]),
		kv(oidKey, oID[:]),
		kv(firstPartKey, firstObj[:]),
		kv(sizeKey, size),
		oidsKV(deletedKey, deleted),
		oidsKV(lockedKey, locked),
		kv(validUntilKey, vub),
	}

	result, err := stackitem.Serialize(stackitem.NewMapWithValue(kvs))
	if err != nil {
		// all the errors in the stackitem relate only cases when it is
		// impossible to use serialized values (too many values, unsupported
		// types, etc.), unexpected errors at all
		panic(fmt.Errorf("unexpected stackitem map serialization failure: %w", err))
	}

	return result, nil
}

func kv(k string, value any) stackitem.MapElement {
	return stackitem.MapElement{
		Key:   stackitem.Make(k),
		Value: stackitem.Make(value),
	}
}

func oidsKV(fieldKey string, oIDs []oid.ID) stackitem.MapElement {
	res := make([]stackitem.Item, 0, len(oIDs))
	for _, oID := range oIDs {
		res = append(res, stackitem.NewByteArray(oID[:]))
	}

	return kv(fieldKey, res)
}
