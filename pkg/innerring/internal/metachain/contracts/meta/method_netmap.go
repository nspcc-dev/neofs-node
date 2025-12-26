package meta

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/common"
)

// Placement is a placeholder for container's storage nodes list.
type Placement []PlacementVector

// PlacementVector is a single placement vector in NeoFS [Placement].
type PlacementVector struct {
	REP   uint8
	Nodes keys.PublicKeys
}

func (p Placement) ToStackItem() (stackitem.Item, error) {
	res := make([]stackitem.Item, 0, len(p))
	for _, v := range p {
		keysRaw := make([]any, 0, len(v.Nodes))
		for i := range v.Nodes {
			keysRaw = append(keysRaw, v.Nodes[i].Bytes())
		}

		res = append(res, stackitem.NewArray([]stackitem.Item{
			stackitem.Make(v.REP),
			stackitem.Make(keysRaw),
		}))
	}

	return stackitem.NewArray(res), nil
}

func (p *Placement) FromStackItem(it stackitem.Item) error {
	arr, ok := it.Value().([]stackitem.Item)
	if !ok {
		return errors.New("not an array")
	}

	*p = make(Placement, 0, len(arr))
	for i := range arr {
		vectorRaw, ok := arr[i].Value().([]stackitem.Item)
		if !ok {
			return fmt.Errorf("%d vector not an array", i)
		}
		if len(vectorRaw) != 2 {
			return fmt.Errorf("%d vector length has unexpected number of fields: %d expected; %d given", i, 2, len(vectorRaw))
		}

		repB, err := vectorRaw[0].TryInteger()
		if err != nil {
			return fmt.Errorf("%d vector has incorrect REP: %w", i, err)
		}
		rep := repB.Int64()
		if rep > maxREPsClauses {
			return fmt.Errorf("%d vector exceeds maximum number of REP: max %d expetected, %d given", i, maxREPsClauses, rep)
		}

		keysRaw, ok := vectorRaw[1].Value().([]stackitem.Item)
		if !ok {
			return fmt.Errorf("%d vector's keys field is not an array: %w", i, err)
		}
		pKeys := make(keys.PublicKeys, 0, len(keysRaw))
		for j := range keysRaw {
			kRaw, err := keysRaw[j].TryBytes()
			if err != nil {
				return fmt.Errorf("incorrect %d key of %d vector: %w", j, i, err)
			}
			var k keys.PublicKey
			err = k.DecodeBytes(kRaw)
			if err != nil {
				return fmt.Errorf("%d key of %d vector is not a key: %w", j, i, err)
			}

			pKeys = append(pKeys, &k)
		}

		*p = append(*p, PlacementVector{REP: uint8(rep), Nodes: pKeys})
	}
	return nil
}

func (m *MetaData) updateContainerList(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 2
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID, ok := args[0].Value().([]byte)
	if !ok {
		panic(fmt.Errorf("unexpected argument value: %T expected, %T given", cID, args[0].Value()))
	}
	if len(cID) != smartcontract.Hash256Len {
		panic(fmt.Errorf("unexpected container ID length: %d expected, %d given", smartcontract.Hash256Len, len(cID)))
	}
	var newPlacement Placement
	err := newPlacement.FromStackItem(args[1])
	if err != nil {
		panic(fmt.Errorf("incorrect placement list: %w", err))
	}

	ok = m.neo.CheckCommittee(ic)
	if !ok {
		panic(common.ErrAlphabetWitnessFailed)
	}

	placementRaw, err := stackitem.Serialize(args[1])
	if err != nil {
		panic(fmt.Errorf("cannot serialize placement: %w", err))
	}

	ic.DAO.PutStorageItem(m.ID, append([]byte{containerPlacementPrefix}, cID...), placementRaw)

	return stackitem.Null{}
}
