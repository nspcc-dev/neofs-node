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
)

// TODO: docs.
type N3ScriptRunner interface {
	InvokeContainedScript(tx *transaction.Transaction, header *block.Header, t *trigger.Type, verbose *bool) (*result.Invoke, error)
}

// TODO: docs.
type FSChain interface {
	N3ScriptRunner
	GetBlockCount() (uint32, error)
}

func verifyN3ScriptsNow(fsChain FSChain, invocScript, verifScript []byte, hashData func() [sha256.Size]byte) error {
	height, err := fsChain.GetBlockCount()
	if err != nil {
		return fmt.Errorf("get FS chain height: %w", err)
	}
	return verifyN3Scripts(fsChain, height, invocScript, verifScript, hashData())
}

func verifyN3Scripts(nsr N3ScriptRunner, height uint32, invocScript, verifScript []byte, dataHash [sha256.Size]byte) error {
	fullScript := slices.Concat(invocScript, verifScript)
	signer := transaction.Signer{} // TODO: needed?

	fakeTx := transaction.NewFakeTX(fullScript, signer, dataHash, 0)
	fakeBlockHdr := &block.Header{
		Index: height,
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
