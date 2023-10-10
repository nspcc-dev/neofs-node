package innerring

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	nnsrpc "github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/privatedomains"
	"github.com/stretchr/testify/require"
)

type testInvoker struct {
	tb           testing.TB
	contractAddr util.Uint160

	domain string

	callRes *result.Invoke
	callErr error
}

func newTestInvoker(tb testing.TB, contractAddr util.Uint160, domain string) *testInvoker {
	return &testInvoker{
		tb:           tb,
		contractAddr: contractAddr,
		domain:       domain,
	}
}

func (x *testInvoker) Call(contract util.Uint160, method string, args ...any) (*result.Invoke, error) {
	require.Equal(x.tb, x.contractAddr, contract)
	require.Equal(x.tb, "resolve", method)
	require.Len(x.tb, args, 2)
	require.Equal(x.tb, x.domain, args[0])
	require.Equal(x.tb, nnsrpc.TXT, args[1])

	if x.callErr != nil {
		return nil, x.callErr
	}

	return x.callRes, nil
}

func (x *testInvoker) CallAndExpandIterator(contract util.Uint160, method string, maxItems int, args ...any) (*result.Invoke, error) {
	panic("not expected to be called")
}

func (x *testInvoker) TerminateSession(sessionID uuid.UUID) error {
	panic("not expected to be called")
}

func (x *testInvoker) TraverseIterator(sessionID uuid.UUID, iterator *result.Iterator, num int) ([]stackitem.Item, error) {
	panic("not expected to be called")
}

func TestNeoFSNNS_CheckDomainRecord(t *testing.T) {
	var contractAddr util.Uint160
	rand.Read(contractAddr[:])
	const domain = "l2.l1.tld"
	searchedRecord := "abcdef"

	t.Run("call failure", func(t *testing.T) {
		inv := newTestInvoker(t, contractAddr, domain)
		inv.callErr = errors.New("any error")

		nnsService := newNeoFSNNS(contractAddr, inv)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorIs(t, err, inv.callErr)
	})

	t.Run("fault exception", func(t *testing.T) {
		inv := newTestInvoker(t, contractAddr, domain)
		inv.callRes = &result.Invoke{
			State:          vmstate.Fault.String(),
			FaultException: "any fault exception",
		}

		nnsService := newNeoFSNNS(contractAddr, inv)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), inv.callRes.FaultException))

		inv.callRes.FaultException = "token not found"

		err = nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorIs(t, err, errDomainNotFound)
	})

	t.Run("invalid stack", func(t *testing.T) {
		inv := newTestInvoker(t, contractAddr, domain)
		inv.callRes = &result.Invoke{
			State: vmstate.Halt.String(),
			Stack: make([]stackitem.Item, 0),
		}

		nnsService := newNeoFSNNS(contractAddr, inv)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorContains(t, err, "result stack is empty")

		inv.callRes.Stack = make([]stackitem.Item, 2)

		err = nnsService.CheckDomainRecord(domain, searchedRecord)
		require.Error(t, err)

		nonArrayItem := stackitem.NewBool(true)
		inv.callRes.Stack = []stackitem.Item{nonArrayItem}

		err = nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorContains(t, err, "not an array")
	})

	newWithRecords := func(items ...stackitem.Item) *neoFSNNS {
		inv := newTestInvoker(t, contractAddr, domain)
		inv.callRes = &result.Invoke{
			State: vmstate.Halt.String(),
			Stack: []stackitem.Item{stackitem.NewArray(items)},
		}

		return newNeoFSNNS(contractAddr, inv)
	}

	t.Run("no domain records", func(t *testing.T) {
		nnsService := newWithRecords()

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorIs(t, err, privatedomains.ErrMissingDomainRecord)
	})

	t.Run("with invalid record", func(t *testing.T) {
		t.Run("non-string element", func(t *testing.T) {
			nnsService := newWithRecords(stackitem.NewMap())

			err := nnsService.CheckDomainRecord(domain, searchedRecord)
			require.ErrorIs(t, err, stackitem.ErrInvalidConversion)
		})
	})

	t.Run("without searched record", func(t *testing.T) {
		records := make([]stackitem.Item, 100)
		for i := range records {
			records[i] = stackitem.NewByteArray([]byte(searchedRecord + hex.EncodeToString([]byte{byte(i)})))
		}

		nnsService := newWithRecords(records...)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorIs(t, err, privatedomains.ErrMissingDomainRecord)
	})

	t.Run("with searched record", func(t *testing.T) {
		records := make([]stackitem.Item, 100)
		for i := range records {
			records[i] = stackitem.NewByteArray([]byte(searchedRecord + hex.EncodeToString([]byte{byte(i)})))
		}

		records = append(records, stackitem.NewByteArray([]byte(searchedRecord)))

		nnsService := newWithRecords(records...)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.NoError(t, err)
	})
}
