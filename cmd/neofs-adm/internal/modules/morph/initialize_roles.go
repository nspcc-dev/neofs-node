package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
)

func (c *initializeContext) setNotaryAndAlphabetNodes() error {
	designateHash, err := c.Client.GetNativeContractHash(nativenames.Designation)
	if err != nil {
		return fmt.Errorf("can't fetch %s hash: %w", nativenames.Designation, err)
	}

	var pubs []interface{}
	for _, w := range c.Wallets {
		acc, err := getWalletAccount(w, singleAccountName)
		if err != nil {
			return err
		}

		pubs = append(pubs, acc.PrivateKey().PublicKey().Bytes())
	}

	w := io.NewBufBinWriter()
	emit.AppCall(w.BinWriter, designateHash, "designateAsRole",
		callflag.States|callflag.AllowNotify, int64(noderoles.P2PNotary), pubs)
	emit.AppCall(w.BinWriter, designateHash, "designateAsRole",
		callflag.States|callflag.AllowNotify, int64(noderoles.NeoFSAlphabet), pubs)

	if err := c.sendCommitteeTx(w.Bytes(), -1); err != nil {
		return err
	}

	return c.awaitTx()
}
