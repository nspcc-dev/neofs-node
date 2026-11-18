package container

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	embeddedcontracts "github.com/nspcc-dev/neofs-contract/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/stretchr/testify/require"
)

type testNotaryEvent struct {
	method string
	params []scparser.PushedItem
	raw    *payload.P2PNotaryRequest
}

func (e testNotaryEvent) ScriptHash() util.Uint160       { return util.Uint160{} }
func (e testNotaryEvent) Type() event.NotaryType         { return event.NotaryTypeFromString(e.method) }
func (e testNotaryEvent) Params() []scparser.PushedItem  { return e.params }
func (e testNotaryEvent) Raw() *payload.P2PNotaryRequest { return e.raw }

// newTestNotaryEvent builds a notary event the same way the preparator does:
// it emits a contract call script and parses its arguments back.
func newTestNotaryEvent(t testing.TB, method string, args ...any) testNotaryEvent {
	w := io.NewBufBinWriter()
	emit.AppCall(w.BinWriter, util.Uint160{}, method, callflag.All, args...)
	require.NoError(t, w.Err)

	_, _, _, params, err := scparser.ParseAppCall(w.Bytes())
	require.NoError(t, err)

	return testNotaryEvent{
		method: method,
		params: params,
		raw:    &payload.P2PNotaryRequest{MainTransaction: &transaction.Transaction{Nonce: 42}},
	}
}

func TestRestoreSetAttributeRequest(t *testing.T) {
	const method = "setAttribute"
	var (
		id    = []byte("container ID")
		invoc = []byte("invocation script")
		verif = []byte("verification script")
		token = []byte("session token")
		orig  = []byte("original request")
	)

	t.Run("without original request", func(t *testing.T) {
		e, err := RestoreSetAttributeRequest(newTestNotaryEvent(t, method, id, "name", "value", int64(100), invoc, verif, token))
		require.NoError(t, err)
		require.Equal(t, SetAttributeRequest{
			MainTransaction:    transaction.Transaction{Nonce: 42},
			ID:                 id,
			Attribute:          "name",
			Value:              "value",
			ValidUntil:         100,
			InvocationScript:   invoc,
			VerificationScript: verif,
			SessionToken:       token,
		}, e)
	})
	t.Run("with original request", func(t *testing.T) {
		e, err := RestoreSetAttributeRequest(newTestNotaryEvent(t, method, id, "name", "value", orig))
		require.NoError(t, err)
		require.Equal(t, SetAttributeRequest{
			MainTransaction: transaction.Transaction{Nonce: 42},
			ID:              id,
			Attribute:       "name",
			Value:           "value",
			OriginalRequest: orig,
		}, e)
	})
	t.Run("wrong argument count", func(t *testing.T) {
		for _, args := range [][]any{
			{id, "name", "value"},
			{id, "name", "value", int64(100), orig},
		} {
			_, err := RestoreSetAttributeRequest(newTestNotaryEvent(t, method, args...))
			require.ErrorContains(t, err, "wrong parameter count")
		}
	})
	t.Run("invalid argument type", func(t *testing.T) {
		_, err := RestoreSetAttributeRequest(newTestNotaryEvent(t, method, id, "name", "value", []any{orig}))
		require.ErrorContains(t, err, "argument #3")
	})
}

func TestRestoreRemoveAttributeRequest(t *testing.T) {
	const method = "removeAttribute"
	var (
		id    = []byte("container ID")
		invoc = []byte("invocation script")
		verif = []byte("verification script")
		token = []byte("session token")
		orig  = []byte("original request")
	)

	t.Run("without original request", func(t *testing.T) {
		e, err := RestoreRemoveAttributeRequest(newTestNotaryEvent(t, method, id, "name", int64(100), invoc, verif, token))
		require.NoError(t, err)
		require.Equal(t, RemoveAttributeRequest{
			MainTransaction:    transaction.Transaction{Nonce: 42},
			ID:                 id,
			Attribute:          "name",
			ValidUntil:         100,
			InvocationScript:   invoc,
			VerificationScript: verif,
			SessionToken:       token,
		}, e)
	})
	t.Run("with original request", func(t *testing.T) {
		e, err := RestoreRemoveAttributeRequest(newTestNotaryEvent(t, method, id, "name", orig))
		require.NoError(t, err)
		require.Equal(t, RemoveAttributeRequest{
			MainTransaction: transaction.Transaction{Nonce: 42},
			ID:              id,
			Attribute:       "name",
			OriginalRequest: orig,
		}, e)
	})
	t.Run("wrong argument count", func(t *testing.T) {
		for _, args := range [][]any{
			{id, "name"},
			{id, "name", int64(100), orig},
		} {
			_, err := RestoreRemoveAttributeRequest(newTestNotaryEvent(t, method, args...))
			require.ErrorContains(t, err, "wrong parameter count")
		}
	})
	t.Run("invalid argument type", func(t *testing.T) {
		_, err := RestoreRemoveAttributeRequest(newTestNotaryEvent(t, method, id, "name", []any{orig}))
		require.ErrorContains(t, err, "argument #2")
	})
}

func TestAttributeRequestParsersMatchContract(t *testing.T) {
	cs, err := embeddedcontracts.GetFS()
	require.NoError(t, err)

	parsers := map[string]func(event.NotaryEvent) (event.Event, error){
		"setAttribute":    RestoreSetAttributeRequest,
		"removeAttribute": RestoreRemoveAttributeRequest,
	}
	for _, c := range cs {
		if c.Manifest.Name != "NeoFS Container" {
			continue
		}
		for _, m := range c.Manifest.ABI.Methods {
			parse, ok := parsers[m.Name]
			if !ok {
				continue
			}

			args := make([]any, len(m.Parameters))
			for i, p := range m.Parameters {
				switch p.Type {
				case smartcontract.IntegerType:
					args[i] = int64(1)
				case smartcontract.StringType:
					args[i] = "1"
				default:
					args[i] = []byte{1}
				}
			}
			_, err := parse(newTestNotaryEvent(t, m.Name, args...))
			require.NoError(t, err, "%s with %d parameters", m.Name, len(m.Parameters))
		}
		return
	}
	t.Fatal("container contract not found")
}
