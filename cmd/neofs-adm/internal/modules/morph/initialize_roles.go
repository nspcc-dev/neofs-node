package morph

import (
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
)

func (c *initializeContext) setNotaryAndAlphabetNodes() error {
	if ok, err := c.setRolesFinished(); ok || err != nil {
		if err == nil {
			c.Command.Println("Stage 2: already performed.")
		}
		return err
	}

	designateHash := c.nativeHash(nativenames.Designation)

	var pubs []interface{}
	for _, acc := range c.Accounts {
		pubs = append(pubs, acc.PrivateKey().PublicKey().Bytes())
	}

	w := io.NewBufBinWriter()
	emit.AppCall(w.BinWriter, designateHash, "designateAsRole",
		callflag.States|callflag.AllowNotify, int64(noderoles.P2PNotary), pubs)
	emit.AppCall(w.BinWriter, designateHash, "designateAsRole",
		callflag.States|callflag.AllowNotify, int64(noderoles.NeoFSAlphabet), pubs)

	if err := c.sendCommitteeTx(w.Bytes(), false); err != nil {
		return err
	}

	return c.awaitTx()
}

func (c *initializeContext) setRolesFinished() (bool, error) {
	height, err := c.Client.GetBlockCount()
	if err != nil {
		return false, err
	}

	h := c.nativeHash(nativenames.Designation)
	pubs, err := getDesignatedByRole(c.ReadOnlyInvoker, h, noderoles.NeoFSAlphabet, height)
	return len(pubs) == len(c.Wallets), err
}
