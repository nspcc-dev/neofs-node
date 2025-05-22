package fschain

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/policy"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
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

	b := smartcontract.NewBuilder()
	for i := range args {
		kv := strings.SplitN(args[i], "=", 2)
		if len(kv) != 2 {
			return errors.New("invalid parameter format, must be Parameter=Value")
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

		b.InvokeMethod(policy.Hash, "set"+kv[0], int64(value))
	}

	script, err := b.Script()
	if err != nil {
		return fmt.Errorf("policy setting script: %w", err)
	}

	if err := wCtx.sendCommitteeTx(script, false); err != nil {
		return err
	}

	return wCtx.awaitTx()
}
