package meta_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neotest"
	"github.com/nspcc-dev/neo-go/pkg/neotest/chain"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts/meta"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

const (
	metaContainersPrefix     = 0x01
	containerPlacementPrefix = 0x02
)

func newMetaClient(t *testing.T) (*neotest.ContractInvoker, *neotest.ContractInvoker) {
	ch, validators, committee := chain.NewMultiWithOptions(t, &chain.Options{
		NewNatives: contracts.NewCustomNatives,
	})
	e := neotest.NewExecutor(t, ch, validators, committee)

	return e.ValidatorInvoker(e.NativeHash(t, meta.MetaDataContractName)), e.CommitteeInvoker(e.NativeHash(t, meta.MetaDataContractName))
}

func TestMetaDataContract_Containers(t *testing.T) {
	metaValidatorsI, metaCommitteeI := newMetaClient(t)
	cID := cidtest.ID()
	key := append([]byte{metaContainersPrefix}, cID[:]...)

	metaValidatorsI.WithSigners(metaValidatorsI.NewAccount(t)).InvokeFail(t, native.ErrInvalidWitness.Error(), "registerMetaContainer", cID[:])

	coldVal := metaValidatorsI.Chain.GetStorageItem(meta.MetaDataContractID, key)
	require.Nil(t, coldVal)

	metaCommitteeI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
	val := metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, key)
	require.Equal(t, []byte{}, []byte(val))

	metaCommitteeI.Invoke(t, stackitem.Null{}, "unregisterMetaContainer", cID[:])
	val = metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, key)
	require.Nil(t, val)
}

func TestMetaDataContract_Netmap(t *testing.T) {
	metaValidatorsI, metaCommitteeI := newMetaClient(t)
	cID := cidtest.ID()

	metaValidatorsI.WithSigners(metaValidatorsI.NewAccount(t)).InvokeFail(t, native.ErrInvalidWitness.Error(), "registerMetaContainer", cID[:])
	metaCommitteeI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
	key := append([]byte{containerPlacementPrefix}, cID[:]...)

	coldVal := metaValidatorsI.Chain.GetStorageItem(meta.MetaDataContractID, key)
	require.Nil(t, coldVal)

	var newPlacement meta.Placement
	for i := range 5 {
		var nodes keys.PublicKeys
		for range 5 {
			k, err := keys.NewPrivateKey()
			require.NoError(t, err)
			nodes = append(nodes, k.PublicKey())
		}

		newPlacement = append(newPlacement, meta.PlacementVector{
			REP:   uint8(i),
			Nodes: nodes,
		})
	}
	newPlacementI, err := newPlacement.ToStackItem()
	require.NoError(t, err)

	metaCommitteeI.Invoke(t, stackitem.Null{}, "updateContainerList", cID[:], &newPlacement)
	val := metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, key)
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
	_, metaCommitteeI := newMetaClient(t)
	cID := cidtest.ID()
	const (
		numOfVectors  = 5
		nodesInVector = 5
	)

	var nodes [][]*keys.PrivateKey
	for range numOfVectors {
		vector := make([]*keys.PrivateKey, 0, nodesInVector)
		for range nodesInVector {
			k, err := keys.NewPrivateKey()
			require.NoError(t, err)

			vector = append(vector, k)
		}
		nodes = append(nodes, vector)
	}
	updateContainerList(t, metaCommitteeI, cID, nodes)

	t.Run("meta disabled", func(t *testing.T) {
		metaInfo, err := stackitem.Serialize(stackitem.NewMapWithValue(
			[]stackitem.MapElement{{Key: stackitem.Make("cid"), Value: stackitem.Make(cID[:])}}))
		require.NoError(t, err)

		metaCommitteeI.InvokeFail(t, "container does not support chained metadata", "submitObjectPut", metaInfo, signMeta([]byte{}, nodes))
	})

	t.Run("meta enabled", func(t *testing.T) {
		oID := oidtest.ID()

		metaCommitteeI.Invoke(t, stackitem.Null{}, "registerMetaContainer", cID[:])
		val := metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, append([]byte{metaContainersPrefix}, cID[:]...))
		require.Equal(t, []byte{}, []byte(val))

		t.Run("not enough signatures", func(t *testing.T) {
			m := testMeta(cID[:], oID[:])
			rawMeta, err := stackitem.Serialize(m)
			require.NoError(t, err)
			sigs := signMeta(rawMeta, nodes)

			badSigI, badSigJ := 2, 3
			sigs[badSigI].([]any)[badSigJ] = make([]byte, keys.SignatureLen)

			metaCommitteeI.InvokeFail(t, fmt.Sprintf("%d sig vector does not contain correct number of signatures, %d found, REP: %d", badSigI, nodesInVector-1, nodesInVector), "submitObjectPut", rawMeta, sigs)
		})

		t.Run("correct meta data", func(t *testing.T) {
			m := testMeta(cID[:], oID[:])
			rawMeta, err := stackitem.Serialize(m)
			require.NoError(t, err)

			sigs := signMeta(rawMeta, nodes)

			hash := metaCommitteeI.Invoke(t, stackitem.Null{}, "submitObjectPut", rawMeta, sigs)
			res := metaCommitteeI.GetTxExecResult(t, hash)
			require.Len(t, res.Events, 1)
			require.Equal(t, "ObjectPut", res.Events[0].Name)
			notificationArgs := res.Events[0].Item.Value().([]stackitem.Item)
			require.Len(t, notificationArgs, 3)
			require.Equal(t, cID[:], notificationArgs[0].Value().([]byte))
			require.Equal(t, oID[:], notificationArgs[1].Value().([]byte))

			metaValuesExp := m.Value().([]stackitem.MapElement)
			metaValuesGot := notificationArgs[2].Value().([]stackitem.MapElement)
			require.Equal(t, metaValuesExp, metaValuesGot)
		})

		t.Run("additional testing values", func(t *testing.T) {
			// meta-on-chain feature is in progress, it may or may not include additional
			// values passed through the contract, therefore, it should be allowed to
			// accept unknown map KV pairs

			m := testMeta(cID[:], oID[:])
			m.Add(stackitem.Make("test"), stackitem.Make("test"))
			rawMeta, err := stackitem.Serialize(m)
			require.NoError(t, err)

			sigs := signMeta(rawMeta, nodes)

			hash := metaCommitteeI.Invoke(t, stackitem.Null{}, "submitObjectPut", rawMeta, sigs)
			res := metaCommitteeI.GetTxExecResult(t, hash)
			require.Len(t, res.Events, 1)
			require.Equal(t, "ObjectPut", res.Events[0].Name)
			notificationArgs := res.Events[0].Item.Value().([]stackitem.Item)
			require.Len(t, notificationArgs, 3)
			require.Equal(t, cID[:], notificationArgs[0].Value().([]byte))
			require.Equal(t, oID[:], notificationArgs[1].Value().([]byte))

			metaValuesExp := m.Value().([]stackitem.MapElement)
			metaValuesGot := notificationArgs[2].Value().([]stackitem.MapElement)
			require.Equal(t, metaValuesExp, metaValuesGot)
		})

		t.Run("missing required values", func(t *testing.T) {
			testFunc := func(key string) {
				m := testMeta(cID[:], oID[:])
				m.Drop(m.Index(stackitem.Make(key)))
				raw, err := stackitem.Serialize(m)
				require.NoError(t, err)

				sigs := signMeta(raw, nodes)

				metaCommitteeI.InvokeFail(t, fmt.Sprintf("missing required '%s' key in map", key), "submitObjectPut", raw, sigs)
			}

			testFunc("oid")
			testFunc("size")
			testFunc("validUntil")
			testFunc("network")
		})

		t.Run("incorrect values", func(t *testing.T) {
			testFunc := func(key string, newVal any) {
				m := testMeta(cID[:], oID[:])
				m.Add(stackitem.Make(key), stackitem.Make(newVal))
				raw, err := stackitem.Serialize(m)
				require.NoError(t, err)

				sigs := signMeta(raw, nodes)

				metaCommitteeI.InvokeFail(t, "incorrect", "submitObjectPut", raw, sigs)
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

func updateContainerList(t *testing.T, metaI *neotest.ContractInvoker, cID cid.ID, nodes [][]*keys.PrivateKey) {
	var newPlacement meta.Placement
	for _, v := range nodes {
		var vectorPublic keys.PublicKeys
		for _, n := range v {
			vectorPublic = append(vectorPublic, n.PublicKey())
		}

		newPlacement = append(newPlacement, meta.PlacementVector{
			REP:   uint8(len(v)),
			Nodes: vectorPublic,
		})
	}

	metaI.Invoke(t, stackitem.Null{}, "updateContainerList", cID[:], &newPlacement)
}

func signMeta(meta []byte, nodes [][]*keys.PrivateKey) []any {
	var sigs []any
	for _, v := range nodes {
		sigsVector := make([]any, 0, len(v))
		for _, n := range v {
			sigsVector = append(sigsVector, n.Sign(meta))
		}

		sigs = append(sigs, sigsVector)
	}

	return sigs
}
