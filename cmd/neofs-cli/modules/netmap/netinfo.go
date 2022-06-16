package netmap

import (
	"encoding/hex"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long:  "Get information about NeoFS network",
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NetworkInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NetworkInfo(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		netInfo := res.NetworkInfo()

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)

		cmd.Printf("Time per block: %s\n", time.Duration(netInfo.MsPerBlock())*time.Millisecond)

		const format = "  %s: %v\n"

		cmd.Println("NeoFS network configuration (system)")
		cmd.Printf(format, "Audit fee", netInfo.AuditFee())
		cmd.Printf(format, "Storage price", netInfo.StoragePrice())
		cmd.Printf(format, "Container fee", netInfo.ContainerFee())
		cmd.Printf(format, "EigenTrust alpha", netInfo.EigenTrustAlpha())
		cmd.Printf(format, "Number of EigenTrust iterations", netInfo.NumberOfEigenTrustIterations())
		cmd.Printf(format, "Epoch duration", netInfo.EpochDuration())
		cmd.Printf(format, "Inner Ring candidate fee", netInfo.IRCandidateFee())
		cmd.Printf(format, "Maximum object size", netInfo.MaxObjectSize())
		cmd.Printf(format, "Withdrawal fee", netInfo.WithdrawalFee())

		cmd.Println("NeoFS network configuration (other)")
		netInfo.IterateRawNetworkParameters(func(name string, value []byte) {
			cmd.Printf(format, name, hex.EncodeToString(value))
		})
	},
}

func initNetInfoCmd() {
	commonflags.Init(netInfoCmd)
	commonflags.InitAPI(netInfoCmd)
}
