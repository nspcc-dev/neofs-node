package contracts

import (
	"fmt"
	"math"
	"testing"

	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neotest"
	"github.com/nspcc-dev/neo-go/pkg/neotest/chain"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func newMetaClient(t *testing.T) *neotest.ContractInvoker {
	ch, committee := chain.NewSingleWithOptions(t, &chain.Options{
		NewNatives: newCustomNatives,
	})
	e := neotest.NewExecutor(t, ch, committee, committee)

	return e.CommitteeInvoker(e.NativeHash(t, MetaDataContractName))
}

func TestMetaDataContract_Containers(t *testing.T) {
	metaI := newMetaClient(t)
	cID := cidtest.ID()
	key := append([]byte{metaContainersPrefix}, cID[:]...)

	metaI.WithSigners(metaI.NewAccount(t)).InvokeFail(t, native.ErrInvalidWitness.Error(), "registerMetaContainer", cID[:])

	coldVal := metaI.Chain.GetStorageItem(MetaDataContractID, key)
	require.Nil(t, coldVal)

	metaI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
	val := metaI.Chain.GetStorageItem(MetaDataContractID, key)
	require.Equal(t, []byte{}, []byte(val))

	metaI.Invoke(t, stackitem.Null{}, "removeMetaContainer", cID[:])
	val = metaI.Chain.GetStorageItem(MetaDataContractID, key)
	require.Nil(t, val)
}

func TestMetaDataContract_Netmap(t *testing.T) {
	metaI := newMetaClient(t)
	cID := cidtest.ID()

	metaI.WithSigners(metaI.NewAccount(t)).InvokeFail(t, native.ErrInvalidWitness.Error(), "registerMetaContainer", cID[:])
	metaI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
	key := append([]byte{containerPlacementPrefix}, cID[:]...)

	coldVal := metaI.Chain.GetStorageItem(MetaDataContractID, key)
	require.Nil(t, coldVal)

	var newPlacement Placement
	for i := range 5 {
		var nodes keys.PublicKeys
		for range 5 {
			k, err := keys.NewPrivateKey()
			require.NoError(t, err)
			nodes = append(nodes, k.PublicKey())
		}

		newPlacement = append(newPlacement, PlacementVector{
			REP:   uint8(i),
			Nodes: nodes,
		})
	}
	newPlacementI, err := newPlacement.ToStackItem()
	require.NoError(t, err)

	metaI.Invoke(t, stackitem.Null{}, "updateContainerList", cID[:], &newPlacement)
	val := metaI.Chain.GetStorageItem(MetaDataContractID, key)
	it, err := stackitem.Deserialize(val)
	require.NoError(t, err)
	requirePlacementsEqual(t, newPlacementI, it)
}

func requirePlacementsEqual(t *testing.T, a, b stackitem.Item) {
	p1, p2 := a.Value().([]stackitem.Item), b.Value().([]stackitem.Item)
	require.Equal(t, len(p1), len(p2))

	for i := range p1 {
		v1 := p1[i].Value().([]stackitem.Item)
		v2 := p2[i].Value().([]stackitem.Item)

		r1, err := v1[0].TryInteger()
		require.NoError(t, err)
		r2, err := v2[0].TryInteger()
		require.NoError(t, err)

		require.Zero(t, r1.Cmp(r2))

		n1 := v1[1].Value().([]stackitem.Item)
		n2 := v2[1].Value().([]stackitem.Item)

		for j := range n1 {
			node1, err := n1[j].TryBytes()
			require.NoError(t, err)
			node2, err := n2[j].TryBytes()
			require.NoError(t, err)

			require.Equal(t, node1, node2)
		}
	}
}

func TestMetaDataContract_Objects(t *testing.T) {
	metaI := newMetaClient(t)
	cID := cidtest.ID()
	const (
		numOfVectors  = 5
		nodesInVector = 5
	)

	var sigs []any
	for range numOfVectors {
		vector := make([]any, 0, nodesInVector)
		for range nodesInVector {
			vector = append(vector, make([]byte, smartcontract.SignatureLen))
		}
		sigs = append(sigs, vector)
	}

	t.Run("meta disabled", func(t *testing.T) {
		metaInfo, err := stackitem.Serialize(stackitem.NewMapWithValue(
			[]stackitem.MapElement{{Key: stackitem.Make("cid"), Value: stackitem.Make(cID[:])}}))
		require.NoError(t, err)

		metaI.InvokeFail(t, "container does not support chained metadata", "submitObjectPut", metaInfo, sigs)
	})

	t.Run("meta enabled", func(t *testing.T) {
		oID := oidtest.ID()

		metaI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
		val := metaI.Chain.GetStorageItem(MetaDataContractID, append([]byte{metaContainersPrefix}, cID[:]...))
		require.Equal(t, []byte{}, []byte(val))

		t.Run("correct meta data", func(t *testing.T) {
			meta := testMeta(cID[:], oID[:])
			rawMeta, err := stackitem.Serialize(meta)
			require.NoError(t, err)

			hash := metaI.Invoke(t, stackitem.Null{}, "submitObjectPut", rawMeta, sigs)
			res := metaI.GetTxExecResult(t, hash)
			require.Len(t, res.Events, 1)
			require.Equal(t, "ObjectPut", res.Events[0].Name)
			notificationArgs := res.Events[0].Item.Value().([]stackitem.Item)
			require.Len(t, notificationArgs, 3)
			require.Equal(t, cID, notificationArgs[0].Value().([]byte))
			require.Equal(t, oID, notificationArgs[1].Value().([]byte))

			metaValuesExp := meta.Value().([]stackitem.MapElement)
			metaValuesGot := notificationArgs[2].Value().([]stackitem.MapElement)
			require.Equal(t, metaValuesExp, metaValuesGot)
		})

		t.Run("additional testing values", func(t *testing.T) {
			// meta-on-chain feature is in progress, it may or may not include additional
			// values passed through the contract, therefore, it should be allowed to
			// accept unknown map KV pairs

			meta := testMeta(cID[:], oID[:])
			meta.Add(stackitem.Make("test"), stackitem.Make("test"))
			rawMeta, err := stackitem.Serialize(meta)
			require.NoError(t, err)

			hash := metaI.Invoke(t, stackitem.Null{}, "submitObjectPut", rawMeta, sigs)
			res := metaI.GetTxExecResult(t, hash)
			require.Len(t, res.Events, 1)
			require.Equal(t, "ObjectPut", res.Events[0].Name)
			notificationArgs := res.Events[0].Item.Value().([]stackitem.Item)
			require.Len(t, notificationArgs, 3)
			require.Equal(t, cID[:], notificationArgs[0].Value().([]byte))
			require.Equal(t, oID, notificationArgs[1].Value().([]byte))

			metaValuesExp := meta.Value().([]stackitem.MapElement)
			metaValuesGot := notificationArgs[2].Value().([]stackitem.MapElement)
			require.Equal(t, metaValuesExp, metaValuesGot)
		})

		t.Run("missing required values", func(t *testing.T) {
			testFunc := func(key string) {
				meta := testMeta(cID[:], oID[:])
				meta.Drop(meta.Index(stackitem.Make(key)))
				raw, err := stackitem.Serialize(meta)
				require.NoError(t, err)
				metaI.InvokeFail(t, fmt.Sprintf("'%s' not found", key), "submitObjectPut", raw, sigs)
			}

			testFunc("oid")
			testFunc("size")
			testFunc("validUntil")
			testFunc("network")
		})

		t.Run("incorrect values", func(t *testing.T) {
			testFunc := func(key string, newVal any) {
				meta := testMeta(cID[:], oID[:])
				meta.Add(stackitem.Make(key), stackitem.Make(newVal))
				raw, err := stackitem.Serialize(meta)
				require.NoError(t, err)
				metaI.InvokeFail(t, "incorrect", "submitObjectPut", raw, sigs)
			}

			testFunc("oid", []byte{1})
			testFunc("validUntil", 1) // tested chain will have some blocks for sure
			testFunc("network", netmode.UnitTestNet+1)
			testFunc("type", math.MaxInt64)
			testFunc("firstPart", []byte{1})
			testFunc("previousPart", []byte{1})
			testFunc("deleted", []any{[]byte{1}})
			testFunc("locked", []any{[]byte{1}})
		})
	})
}

func testMeta(cid, oid []byte) *stackitem.Map {
	deleted := oidtest.ID()
	locked := oidtest.ID()

	return stackitem.NewMapWithValue(
		[]stackitem.MapElement{
			{Key: stackitem.Make("network"), Value: stackitem.Make(netmode.UnitTestNet)},
			{Key: stackitem.Make("cid"), Value: stackitem.Make(cid)},
			{Key: stackitem.Make("oid"), Value: stackitem.Make(oid)},
			{Key: stackitem.Make("firstPart"), Value: stackitem.Make(oid)},
			{Key: stackitem.Make("size"), Value: stackitem.Make(123)},
			{Key: stackitem.Make("deleted"), Value: stackitem.Make([]any{deleted[:]})},
			{Key: stackitem.Make("locked"), Value: stackitem.Make([]any{locked[:]})},
			{Key: stackitem.Make("validUntil"), Value: stackitem.Make(math.MaxInt)},
		})
}

func newCustomNatives(cfg neogoconfig.ProtocolConfiguration) []interop.Contract {
	var (
		defaultContracts = native.NewDefaultContracts(cfg)
		newContracts     = make([]interop.Contract, 0)

		neoContract native.INEO
	)
	for _, contract := range defaultContracts {
		switch contract.(type) {
		case *native.NEO:
			neoContract = contract.(native.INEO)
			newContracts = append(newContracts, neoContract)
		case *native.Management, *native.Ledger, *native.GAS, *native.Policy, *native.Designate, *native.Notary:
			newContracts = append(newContracts, contract)
		case *native.Std, *native.Crypto, *native.Oracle:
		default:
			panic(fmt.Sprintf("unexpected native contract found: %T", contract))
		}
	}
	return append(newContracts, MetaDataContract(neoContract))
}
