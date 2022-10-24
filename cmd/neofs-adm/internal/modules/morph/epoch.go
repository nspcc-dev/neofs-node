package morph

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func forceNewEpochCmd(cmd *cobra.Command, args []string) error {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't to initialize context: %w", err)
	}

	cs, err := wCtx.Client.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't get NNS contract info: %w", err)
	}

	nmHash, err := nnsResolveHash(wCtx.ReadOnlyInvoker, cs.Hash, netmapContract+".neofs")
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	bw := io.NewBufBinWriter()
	if err := emitNewEpochCall(bw, wCtx, nmHash); err != nil {
		return err
	}

	if err := wCtx.sendConsensusTx(bw.Bytes()); err != nil {
		return err
	}

	return wCtx.awaitTx()
}

func emitNewEpochCall(bw *io.BufBinWriter, wCtx *initializeContext, nmHash util.Uint160) error {
	curr, err := unwrap.Int64(wCtx.ReadOnlyInvoker.Call(nmHash, "epoch"))
	if err != nil {
		return errors.New("can't fetch current epoch from the netmap contract")
	}

	newEpoch := curr + 1
	wCtx.Command.Printf("Current epoch: %d, increase to %d.\n", curr, newEpoch)

	// In NeoFS this is done via Notary contract. Here, however, we can form the
	// transaction locally.
	emit.AppCall(bw.BinWriter, nmHash, "newEpoch", callflag.All, newEpoch)
	return bw.Err
}
