package meta_test

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neotest"
	"github.com/nspcc-dev/neo-go/pkg/neotest/chain"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/meta"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

const (
	metaContainersPrefix = iota
	containerPlacementPrefix
)

func newMetaClient(t *testing.T) (*neotest.ContractInvoker, *neotest.ContractInvoker) {
	ch, validators, committee := chain.NewMultiWithOptions(t, &chain.Options{
		NewNatives: metachain.NewCustomNatives,
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
		sort.Sort(nodes)

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
		// not more than 15 signatures are acceptable in invocation script
		numOfVectors  = 3
		nodesInVector = 4
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
	snMultisigner := nodesMultiSigner(metaCommitteeI.Hash, cID, nodes)

	t.Run("meta disabled", func(t *testing.T) {
		oID := oidtest.ID()
		metaInfo, err := stackitem.Serialize(testMeta(cID[:], oID[:]))
		require.NoError(t, err)

		metaCommitteeI.InvokeFail(t, "container does not support chained metadata", "submitObjectPut", metaInfo)
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
			badNodes := slices.Clone(nodes)
			badNodes = badNodes[:len(badNodes)/2]

			verificationFailWithBadSigner(t, metaCommitteeI, nodesMultiSigner(metaCommitteeI.Hash, cID, badNodes), "unexpected", "submitObjectPut", rawMeta)
		})

		t.Run("correct meta data", func(t *testing.T) {
			t.Run("notification", func(t *testing.T) {
				m := testMeta(cID[:], oID[:])
				rawMeta, err := stackitem.Serialize(m)
				require.NoError(t, err)

				h := invokeWithCustomSigner(t, metaCommitteeI, snMultisigner, stackitem.Null{}, "submitObjectPut", rawMeta)
				res := metaCommitteeI.GetTxExecResult(t, h)
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

			t.Run("storage", func(t *testing.T) {
				anotherOID := oidtest.ID()
				lockedObj := oidtest.ID()
				m := testMeta(cID[:], anotherOID[:])
				m.Drop(m.Index(stackitem.Make("deleted")))
				m.Add(stackitem.Make("locked"), stackitem.Make(lockedObj[:]))
				m.Add(stackitem.Make("type"), stackitem.Make(int(object.TypeLock)))

				rawMeta, err := stackitem.Serialize(m)
				require.NoError(t, err)

				invokeWithCustomSigner(t, metaCommitteeI, snMultisigner, stackitem.Null{}, "submitObjectPut", rawMeta)

				k := make([]byte, 1+cid.Size+oid.Size)
				k[0] = 2 // address prefix
				copy(k[1:], cID[:])
				copy(k[1+cid.Size:], anotherOID[:])

				require.Equal(t, rawMeta, []byte(metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, k)))

				k[0] = 3 // lockedBy prefix
				copy(k[1+32:], lockedObj[:])
				require.Equal(t, anotherOID[:], []byte(metaCommitteeI.Chain.GetStorageItem(meta.MetaDataContractID, k)))
			})
		})

		t.Run("additional testing values", func(t *testing.T) {
			// meta-on-chain feature is in progress, it may or may not include additional
			// values passed through the contract, therefore, it should be allowed to
			// accept unknown map KV pairs

			m := testMeta(cID[:], oID[:])
			m.Add(stackitem.Make("test"), stackitem.Make("test"))
			rawMeta, err := stackitem.Serialize(m)
			require.NoError(t, err)

			h := invokeWithCustomSigner(t, metaCommitteeI, snMultisigner, stackitem.Null{}, "submitObjectPut", rawMeta)
			res := metaCommitteeI.GetTxExecResult(t, h)
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

				invokeFailWithCustomSigner(t, metaCommitteeI, snMultisigner, fmt.Sprintf("missing required '%s' key in map", key), "submitObjectPut", raw)
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

				invokeFailWithCustomSigner(t, metaCommitteeI, snMultisigner, "incorrect", "submitObjectPut", raw)
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

	return stackitem.NewMapWithValue(
		[]stackitem.MapElement{
			{Key: stackitem.Make("network"), Value: stackitem.Make(netmode.UnitTestNet)},
			{Key: stackitem.Make("cid"), Value: stackitem.Make(cid)},
			{Key: stackitem.Make("oid"), Value: stackitem.Make(oid)},
			{Key: stackitem.Make("type"), Value: stackitem.Make(1)},
			{Key: stackitem.Make("firstPart"), Value: stackitem.Make(oid)},
			{Key: stackitem.Make("previousPart"), Value: stackitem.Make(oid)},
			{Key: stackitem.Make("size"), Value: stackitem.Make(123)},
			{Key: stackitem.Make("deleted"), Value: stackitem.Make(deleted[:])},
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

func verificationFailWithBadSigner(t testing.TB, validator *neotest.ContractInvoker, customSigner neotest.Signer, message string, method string, args ...any) {
	tx := validator.WithSigners(customSigner).PrepareInvoke(t, method, args...)
	// `VerifyTx`, not `AddNewBlock`, since broken invocation scripts (for test
	// purposes) breaks block chain
	err := validator.Chain.VerifyTx(tx)
	require.ErrorContains(t, err, message)
}

func invokeFailWithCustomSigner(t testing.TB, validator *neotest.ContractInvoker, customSigner neotest.Signer, message string, method string, args ...any) util.Uint256 {
	tx := validator.WithSigners(customSigner).PrepareInvoke(t, method, args...)
	validator.AddNewBlock(t, tx)
	validator.CheckFault(t, tx.Hash(), message)
	return tx.Hash()
}

func invokeWithCustomSigner(t testing.TB, validator *neotest.ContractInvoker, customSigner neotest.Signer, result any, method string, args ...any) util.Uint256 {
	tx := validator.WithSigners(customSigner).PrepareInvoke(t, method, args...)
	validator.AddNewBlock(t, tx)
	validator.CheckHalt(t, tx.Hash(), stackitem.Make(result))
	return tx.Hash()
}

type signer struct {
	verif []byte
	nodes [][]*keys.PrivateKey
}

func (s signer) Script() []byte {
	return s.verif
}

func (s signer) ScriptHash() util.Uint160 {
	return hash.Hash160(s.verif)
}

func (s signer) SignHashable(u uint32, hashable hash.Hashable) []byte {
	var (
		invokBuff = io.NewBufBinWriter()
		writer    = invokBuff.BinWriter
	)
	for i := len(s.nodes) - 1; i >= 0; i-- {
		vectorLen := len(s.nodes[i])
		for j := vectorLen - 1; j >= 0; j-- {
			emit.Bytes(writer, s.nodes[i][j].SignHashable(u, hashable))
		}
		emit.Int(writer, int64(vectorLen))
		emit.Opcodes(writer, opcode.PACK)
	}

	return invokBuff.Bytes()
}

func (s signer) SignTx(magic netmode.Magic, tx *transaction.Transaction) error {
	if len(tx.Signers) != 1 {
		return fmt.Errorf("expected 1 meta signer, got %d", len(tx.Signers))
	}
	if acc := hash.Hash160(s.verif); !tx.Signers[0].Account.Equals(acc) {
		return fmt.Errorf("expected signer %s, got %s", acc, tx.Signers[0].Account)
	}

	// neotest does not support custom scripts and cannot calculate network
	// fee correctly so change it there manually to smth that is currently
	// enough for tests
	tx.NetworkFee = 10_000_000

	tx.Scripts = append(tx.Scripts[:0], transaction.Witness{
		InvocationScript:   s.SignHashable(uint32(magic), tx),
		VerificationScript: s.verif,
	})

	return nil
}

func nodesMultiSigner(contractHash util.Uint160, cID cid.ID, nodes [][]*keys.PrivateKey) neotest.Signer {
	for _, vector := range nodes {
		slices.SortFunc(vector, func(a, b *keys.PrivateKey) int {
			return a.PublicKey().Cmp(b.PublicKey())
		})
	}

	return signer{
		verif: verifScript(contractHash, cID[:], len(nodes)),
		nodes: nodes,
	}
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
