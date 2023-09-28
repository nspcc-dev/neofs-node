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

	traverseErr error

	items     []stackitem.Item
	readItems int
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
	require.Equal(x.tb, "getAllRecords", method)
	require.Len(x.tb, args, 1)
	require.Equal(x.tb, x.domain, args[0])

	if x.callErr != nil {
		return nil, x.callErr
	}

	return x.callRes, nil
}

func (x *testInvoker) CallAndExpandIterator(contract util.Uint160, method string, maxItems int, args ...any) (*result.Invoke, error) {
	panic("not expected to be called")
}

func (x *testInvoker) TerminateSession(sessionID uuid.UUID) error {
	require.Equal(x.tb, x.callRes.Session, sessionID)
	return nil
}

func (x *testInvoker) TraverseIterator(sessionID uuid.UUID, iterator *result.Iterator, num int) ([]stackitem.Item, error) {
	require.Equal(x.tb, x.callRes.Session, sessionID)

	if x.traverseErr != nil {
		return nil, x.traverseErr
	}

	if num > len(x.items)-x.readItems {
		num = len(x.items) - x.readItems
	}

	defer func() {
		x.readItems += num
	}()

	return x.items[x.readItems : x.readItems+num], nil
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
		require.Error(t, err)

		inv.callRes.Stack = make([]stackitem.Item, 2)

		err = nnsService.CheckDomainRecord(domain, searchedRecord)
		require.Error(t, err)

		nonIteratorItem := stackitem.NewBool(true)
		inv.callRes.Stack = []stackitem.Item{nonIteratorItem}

		err = nnsService.CheckDomainRecord(domain, searchedRecord)
		require.Error(t, err)
	})

	newWithRecords := func(items ...stackitem.Item) *neoFSNNS {
		var iter result.Iterator
		inv := newTestInvoker(t, contractAddr, domain)
		inv.callRes = &result.Invoke{
			State: vmstate.Halt.String(),
			Stack: []stackitem.Item{stackitem.NewInterop(iter)},
		}
		inv.items = items

		return newNeoFSNNS(contractAddr, inv)
	}

	t.Run("no domain records", func(t *testing.T) {
		nnsService := newWithRecords()

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.NoError(t, err)
	})

	checkInvalidRecordError := func(record string, err error) {
		var e errInvalidNNSDomainRecord
		require.ErrorAs(t, err, &e)
		require.Equal(t, domain, e.domain)
		require.Equal(t, record, e.record)
	}

	checkInvalidStructFieldError := func(index uint, err error) {
		var e errInvalidStructField
		require.ErrorAs(t, err, &e)
		require.Equal(t, index, e.index)
	}

	t.Run("with invalid record", func(t *testing.T) {
		t.Run("non-struct element", func(t *testing.T) {
			nnsService := newWithRecords(stackitem.NewBool(true))

			err := nnsService.CheckDomainRecord(domain, searchedRecord)

			checkInvalidRecordError("", err)

			var wrongTypeErr errWrongStackItemType
			require.ErrorAs(t, err, &wrongTypeErr)
			require.Equal(t, stackitem.StructT, wrongTypeErr.expected)
			require.Equal(t, stackitem.BooleanT, wrongTypeErr.got)
		})

		t.Run("invalid number of fields", func(t *testing.T) {
			for i := 0; i < 3; i++ {
				nnsService := newWithRecords(stackitem.NewStruct(make([]stackitem.Item, i)))

				err := nnsService.CheckDomainRecord(domain, searchedRecord)

				checkInvalidRecordError("", err)

				var invalidFieldNumErr errInvalidNumberOfStructFields
				require.ErrorAs(t, err, &invalidFieldNumErr)
				require.Equal(t, 3, invalidFieldNumErr.expected, i)
				require.Equal(t, i, invalidFieldNumErr.got, i)
			}
		})

		t.Run("invalid 1st field", func(t *testing.T) {
			nnsService := newWithRecords(stackitem.NewStruct([]stackitem.Item{
				stackitem.NewMap(),
				stackitem.NewBigInteger(nnsrpc.TXT),
				stackitem.NewByteArray([]byte("any")),
			}))

			err := nnsService.CheckDomainRecord(domain, searchedRecord)

			checkInvalidRecordError("", err)
			checkInvalidStructFieldError(0, err)

			var wrongTypeErr errWrongStackItemType
			require.ErrorAs(t, err, &wrongTypeErr)
			require.Equal(t, stackitem.ByteArrayT, wrongTypeErr.expected)
			require.Equal(t, stackitem.MapT, wrongTypeErr.got)
		})

		t.Run("invalid 2nd field", func(t *testing.T) {
			t.Run("non-integer", func(t *testing.T) {
				nnsService := newWithRecords(stackitem.NewStruct([]stackitem.Item{
					stackitem.NewByteArray([]byte("any")),
					stackitem.NewMap(),
					stackitem.NewByteArray([]byte("any")),
				}))

				err := nnsService.CheckDomainRecord(domain, searchedRecord)

				checkInvalidRecordError("", err)
				checkInvalidStructFieldError(1, err)

				var wrongTypeErr errWrongStackItemType
				require.ErrorAs(t, err, &wrongTypeErr)
				require.Equal(t, stackitem.IntegerT, wrongTypeErr.expected)
				require.Equal(t, stackitem.MapT, wrongTypeErr.got)
			})

			t.Run("non-TXT record", func(t *testing.T) {
				nnsService := newWithRecords(stackitem.NewStruct([]stackitem.Item{
					stackitem.NewByteArray([]byte("any")),
					stackitem.NewBigInteger(nnsrpc.CNAME),
					stackitem.NewByteArray([]byte("any")),
				}))

				err := nnsService.CheckDomainRecord(domain, searchedRecord)
				require.ErrorIs(t, err, privatedomains.ErrMissingDomainRecord)
			})
		})

		t.Run("invalid 3rd field", func(t *testing.T) {
			t.Run("invalid type", func(t *testing.T) {
				nnsService := newWithRecords(stackitem.NewStruct([]stackitem.Item{
					stackitem.NewByteArray([]byte("any")),
					stackitem.NewBigInteger(nnsrpc.TXT),
					stackitem.NewMap(),
				}))

				err := nnsService.CheckDomainRecord(domain, searchedRecord)

				checkInvalidRecordError("", err)
				checkInvalidStructFieldError(2, err)

				var wrongTypeErr errWrongStackItemType
				require.ErrorAs(t, err, &wrongTypeErr)
				require.Equal(t, stackitem.ByteArrayT, wrongTypeErr.expected)
				require.Equal(t, stackitem.MapT, wrongTypeErr.got)
			})
		})
	})

	t.Run("without searched record", func(t *testing.T) {
		records := make([]stackitem.Item, 100)
		for i := range records {
			records[i] = stackitem.NewStruct([]stackitem.Item{
				stackitem.NewByteArray([]byte("any")),
				stackitem.NewBigInteger(nnsrpc.TXT),
				stackitem.NewByteArray([]byte(searchedRecord + hex.EncodeToString([]byte{byte(i)}))),
			})
		}

		nnsService := newWithRecords(records...)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.ErrorIs(t, err, privatedomains.ErrMissingDomainRecord)
	})

	t.Run("with searched record", func(t *testing.T) {
		records := make([]stackitem.Item, 100)
		for i := range records {
			records[i] = stackitem.NewStruct([]stackitem.Item{
				stackitem.NewByteArray([]byte("any")),
				stackitem.NewBigInteger(nnsrpc.TXT),
				stackitem.NewByteArray([]byte(searchedRecord + hex.EncodeToString([]byte{byte(i)}))),
			})
		}

		records = append(records, stackitem.NewStruct([]stackitem.Item{
			stackitem.NewByteArray([]byte("any")),
			stackitem.NewBigInteger(nnsrpc.TXT),
			stackitem.NewByteArray([]byte(searchedRecord)),
		}))

		nnsService := newWithRecords(records...)

		err := nnsService.CheckDomainRecord(domain, searchedRecord)
		require.NoError(t, err)
	})
}
