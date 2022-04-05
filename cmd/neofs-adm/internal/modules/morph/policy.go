package morph

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	execFeeParam      = "ExecFeeFactor"
	storagePriceParam = "StoragePrice"
	setFeeParam       = "FeePerByte"
)

func setPolicyCmd(cmd *cobra.Command, args []string) error {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't to initialize context: %w", err)
	}

	policyHash, err := wCtx.Client.GetNativeContractHash(nativenames.Policy)
	if err != nil {
		return fmt.Errorf("can't get policy contract hash: %w", err)
	}

	bw := io.NewBufBinWriter()
	for i := range args {
		kv := strings.SplitN(args[i], "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid parameter format, must be Parameter=Value")
		}

		switch kv[0] {
		case execFeeParam, storagePriceParam, setFeeParam:
		default:
			return fmt.Errorf("parameter must be one of %s, %s and %s", execFeeParam, storagePriceParam, setFeeParam)
		}

		value, err := strconv.ParseUint(kv[1], 10, 32)
		if err != nil {
			return fmt.Errorf("can't parse parameter value '%s': %w", args[1], err)
		}

		emit.AppCall(bw.BinWriter, policyHash, "set"+kv[0], callflag.All, int64(value))
	}

	// Unset group key to use `Global` signer scope. This function is unusual, because we
	// work with native contract, not NeoFS one.
	wCtx.groupKey = nil

	if err := wCtx.sendCommitteeTx(bw.Bytes(), -1); err != nil {
		return err
	}

	return wCtx.awaitTx()
}
