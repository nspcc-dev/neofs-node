package deploy

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/compiler"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/neotest"
	"github.com/nspcc-dev/neo-go/pkg/neotest/chain"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/stretchr/testify/require"
)

func TestVersionCmp(t *testing.T) {
	for _, tc := range []struct {
		xmjr, xmnr, xpatch uint64
		ymjr, ymnr, ypatch uint64
		expected           int
	}{
		{
			1, 2, 3,
			1, 2, 3,
			0,
		},
		{
			0, 1, 1,
			0, 2, 0,
			-1,
		},
		{
			1, 2, 2,
			1, 2, 3,
			-1,
		},
		{
			1, 2, 4,
			1, 2, 3,
			1,
		},
		{
			0, 10, 0,
			1, 2, 3,
			-1,
		},
		{
			2, 0, 0,
			1, 2, 3,
			1,
		},
	} {
		x := contractVersion{tc.xmjr, tc.xmnr, tc.xpatch}
		y := contractVersion{tc.ymjr, tc.ymnr, tc.ypatch}
		require.Equal(t, tc.expected, x.cmp(y), tc)
	}
}

func TestParseContractVersionFromInvocationResult(t *testing.T) {
	var err error
	var res result.Invoke

	// non-HALT state
	_, err = parseContractVersionFromInvocationResult(&res)
	require.Error(t, err)

	res.State = vmstate.Halt.String()

	// empty stack
	_, err = parseContractVersionFromInvocationResult(&res)
	require.Error(t, err)

	// invalid item
	res.Stack = []stackitem.Item{stackitem.Null{}}

	_, err = parseContractVersionFromInvocationResult(&res)
	require.Error(t, err)

	// correct
	ver := contractVersion{1, 2, 3}
	i := new(big.Int)

	res.Stack = []stackitem.Item{stackitem.NewBigInteger(i.SetUint64(ver.toUint64()))}

	// overflow uint64
	i.SetUint64(math.MaxUint64).Add(i, big.NewInt(1))

	_, err = parseContractVersionFromInvocationResult(&res)
	require.Error(t, err)
}

type testRPCInvoker struct {
	invoker.RPCInvoke
	tb   testing.TB
	exec *neotest.Executor
}

func newTestRPCInvoker(tb testing.TB, exec *neotest.Executor) *testRPCInvoker {
	return &testRPCInvoker{
		tb:   tb,
		exec: exec,
	}
}

func (x *testRPCInvoker) InvokeScript(script []byte, _ []transaction.Signer) (*result.Invoke, error) {
	tx := transaction.New(script, 0)
	tx.Nonce = neotest.Nonce()
	tx.ValidUntilBlock = x.exec.Chain.BlockHeight() + 1
	tx.Signers = []transaction.Signer{{Account: x.exec.Committee.ScriptHash()}}

	b := x.exec.NewUnsignedBlock(x.tb, tx)
	ic, err := x.exec.Chain.GetTestVM(trigger.Application, tx, b)
	if err != nil {
		return nil, err
	}
	x.tb.Cleanup(ic.Finalize)

	ic.VM.LoadWithFlags(tx.Script, callflag.All)
	err = ic.VM.Run()
	if err != nil {
		return nil, err
	}

	return &result.Invoke{
		State: vmstate.Halt.String(),
		Stack: ic.VM.Estack().ToArray(),
	}, nil
}

func TestReadContractLocalVersion(t *testing.T) {
	const version = 1_002_003

	bc, acc := chain.NewSingle(t)
	e := neotest.NewExecutor(t, bc, acc, acc)

	src := `package foo
		const version = ` + strconv.Itoa(version) + `
		func Version() int {
			return version
		}`

	ctr := neotest.CompileSource(t, e.CommitteeHash, strings.NewReader(src), &compiler.Options{Name: "Helper"})

	res, err := readContractLocalVersion(newTestRPCInvoker(t, e), *ctr.NEF, *ctr.Manifest)
	require.NoError(t, err)
	require.EqualValues(t, version, res.toUint64())
}
