package meta

import (
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

func (m *MetaData) submitObjectPut(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 2
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
	cID, err := requiredInMap(metaInfo, "cid").TryBytes()
	if err != nil || len(cID) != smartcontract.Hash256Len {
		panic("invalid container ID")
	}
	if ic.DAO.GetStorageItem(m.ID, append([]byte{metaContainersPrefix}, cID...)) == nil {
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

	cnrListRaw := ic.DAO.GetStorageItem(m.ID, append([]byte{containerPlacementPrefix}, cID...))
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

	metaHash := hash.Sha256(metaInfoRaw).BytesBE()
	for i := range sigVectors {
		var foundSigs int
		rep := int(placement[i].REP)
		if len(sigVectors[i]) < rep {
			panic(fmt.Errorf("%d signature vector contains %d signatures but REP is %d", i, len(sigVectors[i]), rep))
		}

		for _, sig := range sigVectors[i] {
			for _, node := range placement[i].Nodes {
				if node.Verify(sig, metaHash) {
					foundSigs++
					break
				}
			}
		}

		if foundSigs < rep {
			panic(fmt.Errorf("%d sig vector does not contain correct number of signatures, %d found, REP: %d", i, foundSigs, rep))
		}
	}

	// required

	oID, err := requiredInMap(metaInfo, "oid").TryBytes()
	if err != nil || len(oID) != smartcontract.Hash256Len {
		panic("incorrect object ID")
	}
	_, err = requiredInMap(metaInfo, "size").TryInteger()
	if err != nil {
		panic("incorrect object size")
	}
	vub, err := requiredInMap(metaInfo, "validUntil").TryInteger()
	if err != nil {
		panic("incorrect vub")
	}
	if v, current := vub.Int64(), ic.BlockHeight(); v <= int64(current) {
		panic(fmt.Sprintf("incorrect vub: %d <= %d (current height)", v, current))
	}
	magic, err := requiredInMap(metaInfo, "network").TryInteger()
	if err != nil {
		panic(fmt.Sprintf("incorrect network magic: %s", err.Error()))
	} else if v, actual := magic.Int64(), ic.Network; v != int64(actual) {
		panic(fmt.Sprintf("incorrect network magic: %d != %d (actual network magic number)", v, actual))
	}

	// optional

	if v, ok := getFromMap(metaInfo, "type"); ok {
		typ, err := v.TryInteger()
		if err != nil {
			panic(fmt.Sprintf("incorrect object type: %s", err.Error()))
		}
		switch typ.Int64() {
		case 0, 1, 2, 3, 4: // regular, tombstone, storage group, lock, link
		default:
			panic(fmt.Errorf("incorrect object type: %d", typ.Int64()))
		}
	}
	if v, ok := getFromMap(metaInfo, "firstPart"); ok {
		_, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Errorf("incorrect first part object ID: %w", err))
		}
	}
	if v, ok := getFromMap(metaInfo, "previousPart"); ok {
		_, err := objectIDFromStackItem(v)
		if err != nil {
			panic(fmt.Errorf("incorrect previous part object ID: %w", err))
		}
	}
	if v, ok := getFromMap(metaInfo, "locked"); ok {
		locked, ok := v.Value().([]stackitem.Item)
		if !ok {
			panic("incorrect locked objects array")
		}
		for i, l := range locked {
			_, err := objectIDFromStackItem(l)
			if err != nil {
				panic(fmt.Errorf("incorrect %d locked object: %w", i, err))
			}
		}
	}
	if v, ok := getFromMap(metaInfo, "deleted"); ok {
		deleted, ok := v.Value().([]stackitem.Item)
		if !ok {
			panic("incorrect deleted objects array")
		}
		for i, d := range deleted {
			_, err := objectIDFromStackItem(d)
			if err != nil {
				panic(fmt.Errorf("incorrect %d deleted object: %w", i, err))
			}
		}
	}

	err = ic.AddNotification(m.Hash, putObjectEvent, stackitem.NewArray([]stackitem.Item{
		stackitem.NewByteArray(cID),
		stackitem.NewByteArray(oID),
		stackitem.NewMapWithValue(metaInfo)}))
	if err != nil {
		panic(err)
	}

	return stackitem.Null{}
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
