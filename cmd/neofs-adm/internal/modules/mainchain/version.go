package mainchain

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-contract/rpc/neofs"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/n3util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var versionCommand = &cobra.Command{
	Use:   "version",
	Short: "Show NeoFS contract version",
	Long: `Invokes version method of the given contract and pretty-prints the result.
Contract is expected to be compatible with NeoFS convention for version() method,
having no parameters and returning an integer.

Example:
  neofs-adm mainchain version \
    --rpc-endpoint http://main-chain.neofs.devenv:30333 \
    --contract-hash Nd7UQEh78WaVdfVaGKu6WaL8Hj9TGQ7Z3J`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: versionCmd,
}

func versionCmd(cmd *cobra.Command, _ []string) error {
	contractHashStr, _ := cmd.Flags().GetString(contractHashFlag)
	if contractHashStr == "" {
		return errors.New("contract hash is required")
	}

	contractHash, err := util.Uint160DecodeStringLE(strings.TrimPrefix(contractHashStr, "0x"))
	if err != nil {
		contractHash, err = address.StringToUint160(contractHashStr)
		if err != nil {
			return fmt.Errorf("invalid contract hash: %w", err)
		}
	}

	c, err := n3util.GetN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}
	inv := invoker.New(c, nil)

	contract := neofs.NewReader(inv, contractHash)

	bigVersion, err := contract.Version()
	if err != nil {
		return fmt.Errorf("can't call version(): %w", err)
	}
	if !bigVersion.IsInt64() {
		return fmt.Errorf("too big version returned: %s", bigVersion)
	}
	v := bigVersion.Int64()
	major := v / 1_000_000
	minor := (v % 1_000_000) / 1000
	patch := v % 1_000

	fmt.Printf("%s: v%d.%d.%d\n", address.Uint160ToString(contractHash), major, minor, patch)
	return nil
}

func initVersionCmd() {
	flags := versionCommand.Flags()
	flags.StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	flags.String(contractHashFlag, "", "Contract hash (address or hex)")

	_ = updateContractCommand.MarkFlagRequired(endpointFlag)
	_ = updateContractCommand.MarkFlagRequired(contractHashFlag)
}
