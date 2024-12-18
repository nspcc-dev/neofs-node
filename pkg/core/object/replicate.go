package object

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const (
	currentVersion = 7 // it is also a number of fields
)

const (
	networkMagicKey = "network"
	cidKey          = "cid"
	oidKey          = "oid"
	sizeKey         = "size"
	deletedKey      = "deleted"
	lockedKey       = "locked"
	validUntilKey   = "validuntil"
)

// EncodeReplicationMetaInfo uses NEO's map (strict order) serialized format as a raw
// representation of object's meta information.
//
// This (ordered) format is used (keys are strings):
//
//	"network": network magic
//	"cid": _raw_ container ID (32 bytes)
//	"oid": _raw_ object ID (32 bytes)
//	"size": payload size
//	"deleted": array of _raw_ object IDs
//	"locked": array of _raw_ object IDs
//	"validuntil": last valid block number for meta information
//
// Last valid epoch is object's creation epoch + 10.
func EncodeReplicationMetaInfo(cID cid.ID, oID oid.ID, pSize uint64,
	deleted, locked []oid.ID, vub uint64, magicNumber uint32) []byte {
	kvs := []stackitem.MapElement{
		kv(networkMagicKey, magicNumber),
		kv(cidKey, cID[:]),
		kv(oidKey, oID[:]),
		kv(sizeKey, pSize),
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
