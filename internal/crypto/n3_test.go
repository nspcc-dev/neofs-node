package crypto_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	isessions "github.com/nspcc-dev/neofs-node/internal/sessions"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

// alwaysHaltTrueRunner simulates a chain node that approves any script
// invocation that halts successfully - mirroring a trivial one-byte PUSH1
// (0x11) verification script, which always halts with a truthy result
// regardless of which account it is claimed to witness for. It isolates the
// account-binding check in verifyN3Scripts from real NeoVM script execution.
type alwaysHaltTrueRunner struct{}

func (alwaysHaltTrueRunner) InvokeContainedScript(*transaction.Transaction, *block.Header, *trigger.Type, *bool) (*result.Invoke, error) {
	return &result.Invoke{
		State: vmstate.Halt.String(),
		Stack: []stackitem.Item{stackitem.NewBool(true)},
	}, nil
}

func (alwaysHaltTrueRunner) GetEpochBlock(uint64) (uint32, error) { return 1, nil }

func (alwaysHaltTrueRunner) GetEpochBlockByTime(uint32) (uint32, error) { return 1, nil }

// pushTrueVerifScript is PUSH1: it halts with a truthy result unconditionally,
// no matter which account it is claimed to witness for.
var pushTrueVerifScript = []byte{0x11}

func TestAuthenticateContainerRequest_N3AccountBinding(t *testing.T) {
	runner := alwaysHaltTrueRunner{}

	t.Run("script not bound to claimed account is rejected", func(t *testing.T) {
		victim := user.NewFromScriptHash(util.Uint160{1, 2, 3}) // unrelated to pushTrueVerifScript
		err := icrypto.AuthenticateContainerRequest(victim, nil, pushTrueVerifScript, []byte("payload"), runner)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not correspond to the expected account")
	})

	t.Run("script bound to the account it actually hashes to is accepted", func(t *testing.T) {
		// Sanity check: the fix doesn't just reject everything - a script
		// claimed for the account it genuinely hashes to still passes.
		selfOwner := user.NewFromScriptHash(hash.Hash160(pushTrueVerifScript))
		err := icrypto.AuthenticateContainerRequest(selfOwner, nil, pushTrueVerifScript, []byte("payload"), runner)
		require.NoError(t, err)
	})
}

// Regression test for the session-token forgery path (C2b): a session token
// claiming an arbitrary issuer, witnessed by a verification script that
// authenticates unconditionally (e.g. PUSH1), must not be accepted as that
// issuer's genuine token.
func TestAuthenticateToken_N3AccountBinding(t *testing.T) {
	victim := user.NewFromScriptHash(util.Uint160{1, 2, 3}) // unrelated to pushTrueVerifScript

	var token session.Container
	token.SetIssuer(victim)
	token.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.N3, pushTrueVerifScript, nil))

	err := icrypto.AuthenticateToken(&token, alwaysHaltTrueRunner{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not correspond to the expected account")
}

// Regression test for the plain (no session token) object-PUT forgery path:
// an object can declare an arbitrary Owner and an N3-scheme signature with a
// verification script that authenticates unconditionally (e.g. PUSH1). This
// must not be accepted as that owner's genuine witness.
func TestAuthenticateObject_N3AccountBinding(t *testing.T) {
	victim := user.NewFromScriptHash(util.Uint160{1, 2, 3}) // unrelated to pushTrueVerifScript

	var obj object.Object
	obj.SetOwner(victim)
	sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.N3, pushTrueVerifScript, nil)
	obj.SetSignature(&sig)

	cache := isessions.NewObjectSessionsCache(1)
	err := icrypto.AuthenticateObject(obj, alwaysHaltTrueRunner{}, cache, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not correspond to the expected account")
}
