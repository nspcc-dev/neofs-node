package objectcore

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

var metaHash = state.CreateNativeContractHash("MetaData")

// TODO
func MetaVerifScript(cID []byte, placementVectorsNumber int) []byte {
	var (
		verifScriptBuf = io.NewBufBinWriter()
		writer         = verifScriptBuf.BinWriter
	)
	emit.Int(writer, int64(placementVectorsNumber)) // sigs array length
	emit.Opcodes(writer, opcode.PACK)
	emit.Bytes(writer, cID)
	emit.Int(writer, 2) // number or args
	emit.Opcodes(writer, opcode.PACK)
	emit.AppCallNoArgs(writer, metaHash, "verifyPlacementSignatures", callflag.ReadOnly)

	return verifScriptBuf.Bytes()
}

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
func EncodeReplicationMetaInfo(numberOfPlacementVectors int, cID cid.ID, oID, firstPart, previousPart oid.ID, pSize uint64, typ object.Type,
	deleted, locked oid.ID, vub uint64, magicNumber uint32) (*transaction.Transaction, []byte) {
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
	if !deleted.IsZero() {
		kvs = append(kvs, kv(deletedKey, deleted))
	}
	if !locked.IsZero() {
		kvs = append(kvs, kv(lockedKey, locked))
	}
	if typ != object.TypeRegular {
		kvs = append(kvs, kv(typeKey, uint32(typ)))
	}

	result, err := stackitem.Serialize(stackitem.NewMapWithValue(kvs))
	if err != nil {
		// all the errors in the stackitem relate only cases when it is
		// impossible to use serialized values (too many values, unsupported
		// types, etc.), unexpected errors at all
		panic(fmt.Errorf("unexpected stackitem map serialization failure: %w", err))
	}

	verifScript := MetaVerifScript(cID[:], numberOfPlacementVectors)
	tx, err := objectTransaction(hash.Hash160(verifScript), result, uint32(vub))
	if err != nil {
		panic(fmt.Errorf("making transaction: %w", err))
	}
	tx.Scripts = append(tx.Scripts[:0], transaction.Witness{
		VerificationScript: verifScript,
	})

	return tx, hash.GetSignedData(magicNumber, tx)
}

// TODO
func objectTransaction(acc util.Uint160, metaData []byte, vub uint32) (*transaction.Transaction, error) {
	script, err := smartcontract.CreateCallScript(metaHash, "submitObjectPut", metaData)
	if err != nil {
		return nil, fmt.Errorf("making transaction script: %w", err)
	}

	tx := transaction.New(script, 0)
	tx.Nonce = vub
	tx.ValidUntilBlock = vub
	tx.Signers = append(tx.Signers, transaction.Signer{
		Account: acc,
		Scopes:  transaction.Global,
	})
	tx.SystemFee = 30 * native.GASFactor
	tx.NetworkFee = 30 * native.GASFactor

	return tx, nil
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
