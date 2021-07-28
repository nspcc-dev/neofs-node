package morph

import (
	"errors"
	"fmt"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
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

	res, err := wCtx.Client.InvokeFunction(cs.Hash, "resolve", []smartcontract.Parameter{
		{Type: smartcontract.StringType, Value: netmapContract + ".neofs"},
		{Type: smartcontract.IntegerType, Value: int64(nns.TXT)},
	}, nil)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}
	if len(res.Stack) == 0 {
		return errors.New("empty response from NNS")
	}

	var nmHash util.Uint160
	bs, err := res.Stack[0].TryBytes()
	if err == nil {
		nmHash, err = util.Uint160DecodeStringLE(string(bs))
	}
	if err != nil {
		return fmt.Errorf("invalid response from NNS contract: %w", err)
	}

	res, err = wCtx.Client.InvokeFunction(nmHash, "epoch", []smartcontract.Parameter{}, []transaction.Signer{{
		Account: wCtx.CommitteeAcc.Contract.ScriptHash(),
		Scopes:  transaction.Global, // Scope is important, as we have nested call to container contract.
	}})
	if err != nil || res.State != vm.HaltState.String() || len(res.Stack) == 0 {
		return errors.New("can't fetch current epoch from the netmap contract")
	}

	bi, err := res.Stack[0].TryInteger()
	if err != nil {
		return fmt.Errorf("can't parse current epoch: %w", err)
	}

	newEpoch := bi.Int64() + 1
	cmd.Printf("Current epoch: %s, increase to %d.\n", bi, newEpoch)

	// In NeoFS this is done via Notary contract. Here, however, we can form the
	// transaction locally.
	bw := io.NewBufBinWriter()
	emit.AppCall(bw.BinWriter, nmHash, "newEpoch", callflag.All, newEpoch)
	if err := wCtx.sendCommitteeTx(bw.Bytes(), -1); err != nil {
		return err
	}

	return wCtx.awaitTx()
}
