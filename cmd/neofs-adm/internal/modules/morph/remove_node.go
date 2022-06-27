package morph

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	netmapcontract "github.com/nspcc-dev/neofs-contract/netmap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func removeNodesCmd(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return errors.New("at least one node key must be provided")
	}

	nodeKeys := make(keys.PublicKeys, len(args))
	for i := range args {
		var err error
		nodeKeys[i], err = keys.NewPublicKeyFromString(args[i])
		if err != nil {
			return fmt.Errorf("can't parse node public key: %w", err)
		}
	}

	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't initialize context: %w", err)
	}

	cs, err := wCtx.Client.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't get NNS contract info: %w", err)
	}

	nmHash, err := nnsResolveHash(wCtx.Client, cs.Hash, netmapContract+".neofs")
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	bw := io.NewBufBinWriter()
	for i := range nodeKeys {
		emit.AppCall(bw.BinWriter, nmHash, "updateStateIR", callflag.All,
			int64(netmapcontract.OfflineState), nodeKeys[i].Bytes())
	}

	if err := emitNewEpochCall(bw, wCtx, nmHash); err != nil {
		return err
	}

	if err := wCtx.sendCommitteeTx(bw.Bytes(), -1, true); err != nil {
		return err
	}

	return wCtx.awaitTx()
}
