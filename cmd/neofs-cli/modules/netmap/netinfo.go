package netmap

import (
	"encoding/hex"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

type netCfgWriter cobra.Command

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

		netCfg := netInfo.NetworkConfig()

		cmd.Println("NeoFS network configuration")

		err = nmClient.WriteConfig((*netCfgWriter)(cmd), func(f func(key []byte, val []byte) error) error {
			var err error

			netCfg.IterateParameters(func(prm *netmap.NetworkParameter) bool {
				err = f(prm.Key(), prm.Value())
				return err != nil
			})

			return err
		})
		common.ExitOnErr(cmd, "read config: %w", err)
	},
}

func initNetInfoCmd() {
	commonflags.Init(netInfoCmd)
	commonflags.InitAPI(netInfoCmd)
}

func (x *netCfgWriter) print(name string, v interface{}, unknown bool) {
	var sUnknown string

	if unknown {
		sUnknown = " (unknown)"
	}

	(*cobra.Command)(x).Printf("  %s%s: %v\n", name, sUnknown, v)
}

func (x *netCfgWriter) UnknownParameter(k string, v []byte) {
	x.print(k, hex.EncodeToString(v), true)
}

func (x *netCfgWriter) MaxObjectSize(v uint64) {
	x.print("Maximum object size", v, false)
}

func (x *netCfgWriter) BasicIncomeRate(v uint64) {
	x.print("Basic income rate", v, false)
}

func (x *netCfgWriter) AuditFee(v uint64) {
	x.print("Audit fee", v, false)
}

func (x *netCfgWriter) EpochDuration(v uint64) {
	x.print("Epoch duration", v, false)
}

func (x *netCfgWriter) ContainerFee(v uint64) {
	x.print("Container fee", v, false)
}

func (x *netCfgWriter) ContainerAliasFee(v uint64) {
	x.print("Container alias fee", v, false)
}

func (x *netCfgWriter) EigenTrustIterations(v uint64) {
	x.print("Number EigenTrust of iterations", v, false)
}

func (x *netCfgWriter) EigenTrustAlpha(v float64) {
	x.print("EigenTrust α", v, false)
}

func (x *netCfgWriter) InnerRingCandidateFee(v uint64) {
	x.print("Inner Ring candidate fee", v, false)
}

func (x *netCfgWriter) WithdrawFee(v uint64) {
	x.print("Withdraw fee", v, false)
}
