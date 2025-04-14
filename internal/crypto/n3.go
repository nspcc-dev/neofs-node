package crypto

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// N3ScriptRunner allows to makes historic N3 script runs on the N3 chain.
type N3ScriptRunner interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, _ *trigger.Type, _ *bool) (*result.Invoke, error)
}

func verifyN3ScriptsNow(nsr N3ScriptRunner, acc util.Uint160, invocScript, verifScript []byte, hashData func() [sha256.Size]byte) error {
	return verifyN3Scripts(nsr, 0, acc, invocScript, verifScript, hashData())
}

func verifyN3Scripts(nsr N3ScriptRunner, height uint32, acc util.Uint160, invocScript, verifScript []byte, dataHash [sha256.Size]byte) error {
	fullScript := slices.Concat(invocScript, verifScript)
	signer := transaction.Signer{
		Account: acc,
		Scopes:  transaction.None,
	}

	fakeTx := transaction.NewFakeTX(fullScript, signer, dataHash, 0)
	var fakeBlockHdr *block.Header
	if height > 0 {
		fakeBlockHdr = &block.Header{
			Index: height,
		}
	}

	ok, err := unwrap.Bool(nsr.InvokeContainedScript(fakeTx, fakeBlockHdr, nil, nil))
	if err != nil {
		return fmt.Errorf("run verification script: %w", err)
	}

	if !ok {
		return errors.New("verification script run resulted in false")
	}

	return nil
}
