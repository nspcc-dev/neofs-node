package object

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const (
	// required fields.
	cidKey          = "cid"
	oidKey          = "oid"
	sizeKey         = "size"
	validUntilKey   = "validUntil"
	networkMagicKey = "network"

	// optional fields.
	firstPartKey    = "firstPart"
	previousPartKey = "previousPart"
	deletedKey      = "deleted"
	lockedKey       = "locked"
	typeKey         = "type"
)

// EncodeReplicationMetaInfo uses NEO's map (strict order) serialized format as a raw
// representation of object's meta information.
//
// This (ordered) format is used (keys are strings):
//
//	"cid": _raw_ container ID (32 bytes)
//	"oid": _raw_ object ID (32 bytes)
//	"size": payload size
//	"validUntil": last valid block number for meta information
//	"network": network magic
//	"firstPart": [OPTIONAL] _raw_ object ID (32 bytes)
//	"previousPart": [OPTIONAL] _raw_ object ID (32 bytes)
//	"deleted": [OPTIONAL] array of _raw_ object IDs
//	"locked": [OPTIONAL] array of _raw_ object IDs
//	"type": [OPTIONAL] object type enumeration
func EncodeReplicationMetaInfo(cID cid.ID, oID, firstPart, previousPart oid.ID, pSize uint64, typ objectsdk.Type,
	deleted, locked []oid.ID, vub uint64, magicNumber uint32) []byte {
	kvs := []stackitem.MapElement{
		kv(cidKey, cID[:]),
		kv(oidKey, oID[:]),
		kv(sizeKey, pSize),
		kv(validUntilKey, vub),
		kv(networkMagicKey, magicNumber),
	}

	if !firstPart.IsZero() {
		kvs = append(kvs, kv(firstPartKey, firstPart[:]))
	}
	if !previousPart.IsZero() {
		kvs = append(kvs, kv(previousPartKey, previousPart[:]))
	}
	if len(deleted) > 0 {
		kvs = append(kvs, oidsKV(deletedKey, deleted))
	}
	if len(locked) > 0 {
		kvs = append(kvs, oidsKV(lockedKey, locked))
	}
	if typ != objectsdk.TypeRegular {
		kvs = append(kvs, kv(typeKey, uint32(typ)))
	}

	result, err := stackitem.Serialize(stackitem.NewMapWithValue(kvs))
	if err != nil {
		// all the errors in the stackitem relate only cases when it is
		// impossible to use serialized values (too many values, unsupported
		// types, etc.), unexpected errors at all
		panic(fmt.Errorf("unexpected stackitem map serialization failure: %w", err))
	}

	return result
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
