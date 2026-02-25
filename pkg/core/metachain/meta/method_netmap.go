package meta

import (
	"errors"
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/common"
)

// Placement is a placeholder for container's storage nodes list.
type Placement []PlacementVector

func (p Placement) ToSCParameter() (smartcontract.Parameter, error) {
	res := smartcontract.NewParameter(smartcontract.ArrayType)

	scVectors := make([]smartcontract.Parameter, 0, len(p))
	for i, v := range p {
		scNodes := make([]smartcontract.Parameter, 0, len(v.Nodes))
		for j, n := range v.Nodes {
			scNode, err := smartcontract.NewParameterFromValue(n.Bytes())
			if err != nil {
				return smartcontract.Parameter{}, fmt.Errorf("converting %d node of %d vector: %w", j, i, err)
			}
			scNodes = append(scNodes, scNode)
		}

		scRep, err := smartcontract.NewParameterFromValue(v.REP)
		if err != nil {
			return smartcontract.Parameter{}, fmt.Errorf("converting REP of %d vector: %w", i, err)
		}

		scNodesField, err := smartcontract.NewParameterFromValue(scNodes)
		if err != nil {
			return smartcontract.Parameter{}, fmt.Errorf("converting %d vector nodes to struct field: %w", i, err)
		}

		scVector := smartcontract.NewParameter(smartcontract.ArrayType)
		scVector.Value = []smartcontract.Parameter{scRep, scNodesField}

		scVectors = append(scVectors, scVector)
	}

	res.Value = scVectors

	return res, nil
}

// PlacementVector is a single placement vector in NeoFS [Placement].
type PlacementVector struct {
	REP   uint8
	Nodes keys.PublicKeys
}

func (p PlacementVector) ToStackItem() (stackitem.Item, error) {
	nodes := make([]stackitem.Item, 0, len(p.Nodes))
	for _, node := range p.Nodes {
		nodes = append(nodes, stackitem.NewByteArray(node.Bytes()))
	}

	return stackitem.NewStruct([]stackitem.Item{
		stackitem.Make(p.REP),
		stackitem.NewArray(nodes),
	}), nil
}

func (p *PlacementVector) FromStackItem(it stackitem.Item) error {
	arr, ok := it.Value().([]stackitem.Item)
	if !ok {
		return errors.New("not an array")
	}
	if len(arr) != 2 {
		return fmt.Errorf("unexpected number of fields: %d expected; %d given", 2, len(arr))
	}

	rep, err := stackitem.ToUint8(arr[0])
	if err != nil {
		return fmt.Errorf("first fiels not an uint8: %w", err)
	}
	p.REP = rep

	p.Nodes = make(keys.PublicKeys, 0, len(arr))
	vectorRaw, ok := arr[1].Value().([]stackitem.Item)
	if !ok {
		return fmt.Errorf("second field is not an array")
	}
	for i := range vectorRaw {
		kRaw, err := vectorRaw[i].TryBytes()
		if err != nil {
			return fmt.Errorf("incorrect %d key: %w", i, err)
		}
		var k keys.PublicKey
		err = k.DecodeBytes(kRaw)
		if err != nil {
			return fmt.Errorf("%d key is not a key: %w", i, err)
		}

		p.Nodes = append(p.Nodes, &k)
	}

	return nil
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

	*p = make(Placement, len(arr))
	for i := range arr {
		err := (*p)[i].FromStackItem(arr[i])
		if err != nil {
			return fmt.Errorf("parsing %d placement vector: %w", i, err)
		}
	}
	return nil
}

func (m *MetaData) updateContainerList(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	const argsNumber = 2
	if len(args) != argsNumber {
		panic(fmt.Errorf("unexpected number of args: %d expected, %d given", argsNumber, len(args)))
	}
	cID, err := stackitem.ToUint256(args[0])
	if err != nil {
		panic(err)
	}

	var newPlacement Placement
	err = newPlacement.FromStackItem(args[1])
	if err != nil {
		panic(fmt.Errorf("incorrect placement list: %w", err))
	}

	ok := m.neo.CheckCommittee(ic)
	if !ok {
		panic(common.ErrAlphabetWitnessFailed)
	}

	for _, vector := range newPlacement {
		sort.Sort(vector.Nodes)
	}

	err = ic.DAO.PutStorageConvertible(m.ID, append([]byte{containerPlacementPrefix}, cID[:]...), &newPlacement)
	if err != nil {
		panic(fmt.Errorf("cannot put updated placement: %w", err))
	}

	return stackitem.Null{}
}
