package meta

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
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

type objectMeta struct {
	cID  []byte
	oID  []byte
	size uint64
	typ  uint8

	firstPart    []byte
	previousPart []byte
	locked       []byte
	deleted      []byte
}

func (m *objectMeta) parse(ic *interop.Context, metaInfo []stackitem.MapElement) error {
	// required

	cID, err := requiredInMap(metaInfo, "cid").TryBytes()
	if err != nil || len(cID) != smartcontract.Hash256Len {
		panic("invalid container ID")
	}
	m.cID = cID

	oID, err := requiredInMap(metaInfo, "oid").TryBytes()
	if err != nil || len(oID) != smartcontract.Hash256Len {
		panic("incorrect object ID")
	}
	m.oID = oID

	sizeB, err := requiredInMap(metaInfo, "size").TryInteger()
	if err != nil {
		panic("incorrect object size")
	}
	size := sizeB.Int64()
	if size < 0 {
		panic(fmt.Sprintf("negative object size: %d", size))
	}
	m.size = uint64(size)

	vub, err := requiredInMap(metaInfo, "validUntil").TryInteger()
	if err != nil {
		panic("incorrect vub")
	}
	if v, current := vub.Int64(), ic.BlockHeight(); v <= int64(current) {
		panic(fmt.Sprintf("incorrect vub: object cannot be accepted: %d <= %d (current height)", v, current))
	}

	magic, err := requiredInMap(metaInfo, "network").TryInteger()
	if err != nil {
		panic(fmt.Sprintf("incorrect network magic: %s", err.Error()))
	}
	if v, actual := magic.Int64(), ic.Network; v != int64(actual) {
		panic(fmt.Sprintf("incorrect network magic: %d != %d (actual network magic number)", v, actual))
	}

	// optional
	if v, ok := getFromMap(metaInfo, "type"); ok {
		typ, err := v.TryInteger()
		if err != nil {
			panic(fmt.Sprintf("incorrect object type: %s", err.Error()))
		}
		switch object.Type(typ.Int64()) {
		case object.TypeRegular, object.TypeTombstone, object.TypeLock, object.TypeLink:
		default:
			panic(fmt.Errorf("incorrect object type: %d", typ.Int64()))
		}
		m.typ = uint8(typ.Int64())
	}

	if v, ok := getFromMap(metaInfo, "firstPart"); ok {
		first, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Errorf("incorrect first part object ID: %w", err))
		}
		m.firstPart = first[:]
	}
	if v, ok := getFromMap(metaInfo, "previousPart"); ok {
		prev, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Errorf("incorrect previous part object ID: %w", err))
		}
		m.previousPart = prev[:]
	}
	if v, ok := getFromMap(metaInfo, "locked"); ok {
		locked, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Sprintf("incorrect locked object: %s", err))
		}
		if m.typ != 3 {
			panic("non-LOCK object with associated locked object")
		}

		m.locked = locked[:]
	}
	if v, ok := getFromMap(metaInfo, "deleted"); ok {
		deleted, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Sprintf("incorrect deleted object: %s", err))
		}
		if m.typ != 1 {
			panic("non-TS object with associated deleted object")
		}

		m.deleted = deleted[:]
	}

	return nil
}

func (m *MetaData) submitObjectPut(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 1
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	metaInfoRaw, ok := args[0].Value().([]byte)
	if !ok {
		panic(fmt.Errorf("unexpected first argument value: %T expected, %T given", metaInfoRaw, args[0].Value()))
	}
	metaInfoSI, err := stackitem.Deserialize(metaInfoRaw)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize meta information from byte array: %w", err))
	}
	metaInfo, ok := metaInfoSI.Value().([]stackitem.MapElement)
	if !ok {
		panic(fmt.Errorf("unexpected deserialized meta information value: expected %T, %T given", metaInfo, metaInfoSI.Value()))
	}

	var o objectMeta
	err = o.parse(ic, metaInfo)
	if err != nil {
		panic(err)
	}

	if ic.DAO.GetStorageItem(m.ID, append([]byte{metaContainersPrefix}, o.cID...)) == nil {
		panic("container does not support chained metadata")
	}

	cnrListRaw := ic.DAO.GetStorageItem(m.ID, append([]byte{containerPlacementPrefix}, o.cID...))
	placementI, err := stackitem.Deserialize(cnrListRaw)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize container placement list: %w", err))
	}
	var placement Placement
	err = placement.FromStackItem(placementI)
	if err != nil {
		panic(fmt.Errorf("cannot retrieve placement vector from stack item: %w", err))
	}

	err = isSignedBySNs(ic, m.Hash, o.cID, len(placement))
	if err != nil {
		panic(err)
	}

	err = storeObject(ic, o, metaInfoRaw)
	if err != nil {
		panic(fmt.Errorf("cannot store %s/%s object: %w", base58.Encode(o.cID), base58.Encode(o.oID), err))
	}

	err = ic.AddNotification(m.Hash, putObjectEvent, stackitem.NewArray([]stackitem.Item{
		stackitem.NewByteArray(o.cID),
		stackitem.NewByteArray(o.oID),
		stackitem.NewMapWithValue(metaInfo)}))
	if err != nil {
		panic(err)
	}

	return stackitem.Null{}
}

func storageKey(storagePrefix storage.KeyPrefix, contractID int32, key []byte) []byte {
	// 1 for prefix + 4 for Uint32 + len(key) for key
	k := make([]byte, 5+len(key))
	k[0] = byte(storagePrefix)
	binary.LittleEndian.PutUint32(k[1:], uint32(contractID))
	copy(k[5:], key)

	return k
}

func putStorageItem(ic *interop.Context, key, value []byte) {
	ic.DAO.Store.Put(storageKey(ic.DAO.Version.StoragePrefix, MetaDataContractID, key), value)
}

func getStorageItem(ic *interop.Context, key []byte) state.StorageItem {
	v, err := ic.DAO.Store.Get(storageKey(ic.DAO.Version.StoragePrefix, MetaDataContractID, key))
	if err != nil {
		return nil
	}
	return v
}

func deleteStorageItem(ic *interop.Context, key []byte) {
	ic.DAO.Store.Delete(storageKey(ic.DAO.Version.StoragePrefix, MetaDataContractID, key))
}

func storeObject(ic *interop.Context, parsed objectMeta, rawMeta []byte) error {
	key := make([]byte, 1+cid.Size+oid.Size)
	copy(key[1:], parsed.cID)

	if parsed.deleted != nil {
		key[0] = lockedByIndex
		copy(key[1+32:], parsed.deleted)

		if l := getStorageItem(ic, key); l != nil {
			return errors.New("locked object deletion")
		}

		key[0] = addrIndex
		deleteStorageItem(ic, key)
	}

	copy(key[1+32:], parsed.oID)

	key[0] = addrIndex
	putStorageItem(ic, key, rawMeta)

	if parsed.locked != nil {
		key[0] = lockedByIndex
		copy(key[1+32:], parsed.locked)
		putStorageItem(ic, key, parsed.oID)
	}

	return nil
}

func (m *MetaData) verifyPlacementSignatures(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	var (
		signedData = make([]byte, 4, 4+util.Uint256Size)
		h          = ic.Container.Hash()
	)
	binary.LittleEndian.PutUint32(signedData, ic.Network)
	signedData = append(signedData, h[:]...)
	signedDataHash := sha256.Sum256(signedData)

	const expectedNumberOfArgs = 2
	if len(args) != expectedNumberOfArgs {
		return stackitem.NewBool(false)
	}

	cID, err := stackitem.ToUint256(args[0])
	if err != nil {
		panic(err)
	}
	if ic.DAO.GetStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID[:]...)) == nil {
		panic("container does not support chained metadata")
	}

	sigsVectorsRaw, ok := args[1].Value().([]stackitem.Item)
	if !ok {
		panic(fmt.Errorf("unexpected second argument value: %T expected, %T given", sigsVectorsRaw, args[1].Value()))
	}
	var sigVectors = make([][][]byte, 0, len(sigsVectorsRaw))
	for i := range sigsVectorsRaw {
		vectorRaw, ok := sigsVectorsRaw[i].Value().([]stackitem.Item)
		if !ok {
			panic(fmt.Errorf("unexpected %d signatures vector value: %T expected, %T given", i, vectorRaw, sigsVectorsRaw[i].Value()))
		}
		vector := make([][]byte, 0, len(vectorRaw))
		for j := range vectorRaw {
			sig, ok := vectorRaw[j].Value().([]byte)
			if !ok {
				panic(fmt.Errorf("unexpected %d signature value in %d signatures vector: %T expected, %T given", j, i, sig, sigsVectorsRaw[j].Value()))
			}
			vector = append(vector, sig)
		}
		sigVectors = append(sigVectors, vector)
	}

	cnrListRaw := ic.DAO.GetStorageItem(m.ID, append([]byte{containerPlacementPrefix}, cID[:]...))
	placementI, err := stackitem.Deserialize(cnrListRaw)
	if err != nil {
		panic(fmt.Errorf("cannot deserialize container placement list: %w", err))
	}
	var placement Placement
	err = placement.FromStackItem(placementI)
	if err != nil {
		panic(fmt.Errorf("cannot retrieve placement vector from stack item: %w", err))
	}
	if len(sigVectors) != len(placement) {
		panic(fmt.Errorf("unexpected number of signature vectors: %d signatures, %d placement vectors found", len(sigVectors), len(placement)))
	}

	for i, vector := range placement {
		var foundSigs, lastFoundSig int
		for _, sig := range sigVectors[i] {
			// placement nodes are sorted by their public keys, so the signers are expected to be
			for j := max(0, lastFoundSig); j < len(vector.Nodes); j++ {
				if vector.Nodes[j].Verify(sig, signedDataHash[:]) {
					foundSigs++
					lastFoundSig = j
					break
				}
			}
			if foundSigs == int(vector.REP) {
				break
			}
		}
		if foundSigs < int(vector.REP) {
			panic(fmt.Sprintf("REP %d is not sufficient for %d placement vector, %d signatures found", vector.REP, i, foundSigs))
		}
	}

	return stackitem.NewBool(true)
}

func verifScript(hash util.Uint160, cID []byte, placementVectorsNumber int) []byte {
	var (
		verifScriptBuf = io.NewBufBinWriter()
		writer         = verifScriptBuf.BinWriter
	)
	emit.Int(writer, int64(placementVectorsNumber)) // sigs array length
	emit.Opcodes(writer, opcode.PACK)
	emit.Bytes(writer, cID)
	emit.Int(writer, 2) // number or args
	emit.Opcodes(writer, opcode.PACK)
	emit.AppCallNoArgs(writer, hash, "verifyPlacementSignatures", callflag.ReadOnly)

	return verifScriptBuf.Bytes()
}

func isSignedBySNs(ic *interop.Context, contract util.Uint160, cID []byte, placementVectorsNumber int) error {
	if l := len(ic.Tx.Scripts); l != 1 {
		return fmt.Errorf("expected exactly 1 witness script, got %d", l)
	}

	acc := hash.Hash160(verifScript(contract, cID, placementVectorsNumber))
	if !ic.Tx.Signers[0].Account.Equals(acc) {
		return fmt.Errorf("not signed by %s account", acc)
	}

	return nil
}

func requiredInMap(m []stackitem.MapElement, key string) stackitem.Item {
	v, ok := getFromMap(m, key)
	if !ok {
		panic("missing required '" + key + "' key in map")
	}

	return v
}

func getFromMap(m []stackitem.MapElement, key string) (stackitem.Item, bool) {
	k := stackitem.Make(key)
	i := slices.IndexFunc(m, func(e stackitem.MapElement) bool {
		return e.Key.Equals(k)
	})
	if i == -1 {
		return nil, false
	}

	return m[i].Value, true
}
