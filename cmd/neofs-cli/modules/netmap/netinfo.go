package netmap

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
)

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long: `Get information about NeoFS network.
		Unknown configuration settings are displayed in hexadecimal format.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		netInfo, err := cli.NetworkInfo(ctx, client.PrmNetworkInfo{})
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)

		cmd.Printf("Time per block: %s\n", time.Duration(netInfo.MsPerBlock())*time.Millisecond)

		const format = "  %s: %v\n"

		cmd.Println("NeoFS network configuration (system)")
		cmd.Printf(format, "Storage price", netInfo.StoragePrice())
		cmd.Printf(format, "Container fee", netInfo.ContainerFee())
		cmd.Printf(format, "Container alias fee", netInfo.NamedContainerFee())
		cmd.Printf(format, "EigenTrust alpha", netInfo.EigenTrustAlpha())
		cmd.Printf(format, "Number of EigenTrust iterations", netInfo.NumberOfEigenTrustIterations())
		cmd.Printf(format, "Epoch duration", netInfo.EpochDuration())
		cmd.Printf(format, "Maximum object size", netInfo.MaxObjectSize())
		cmd.Printf(format, "Withdrawal fee", netInfo.WithdrawalFee())
		cmd.Printf(format, "Homomorphic hashing disabled", netInfo.HomomorphicHashingDisabled())

		cmd.Println("NeoFS network configuration (other)")
		for name, value := range netInfo.RawNetworkParameters() {
			cmd.Printf(format, name, hex.EncodeToString(value))
		}
		return nil
	},
}

func initNetInfoCmd() {
	commonflags.Init(netInfoCmd)
	commonflags.InitAPI(netInfoCmd)
}
