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
		k, v, found := strings.Cut(args[i], "=")
		if !found {
			return errors.New("invalid parameter format, must be Parameter=Value")
		}

		switch k {
		case execFeeParam, storagePriceParam, setFeeParam:
		default:
			return fmt.Errorf("parameter must be one of %s, %s and %s", execFeeParam, storagePriceParam, setFeeParam)
		}

		value, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return fmt.Errorf("can't parse parameter value '%s': %w", v, err)
		}

		b.InvokeMethod(policy.Hash, "set"+k, int64(value))
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
