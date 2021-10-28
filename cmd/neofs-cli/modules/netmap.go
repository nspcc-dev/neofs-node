package cmd

import (
	"encoding/hex"
	"time"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var (
	nodeInfoJSON bool

	netmapSnapshotJSON bool
)

// netmapCmd represents the netmap command
var netmapCmd = &cobra.Command{
	Use:   "netmap",
	Short: "Operations with Network Map",
	Long:  `Operations with Network Map`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		bindCommonFlags(cmd)
		bindAPIFlags(cmd)
	},
}

func init() {
	netmapChildCommands := []*cobra.Command{
		getEpochCmd,
		localNodeInfoCmd,
		netInfoCmd,
	}

	rootCmd.AddCommand(netmapCmd)
	netmapCmd.AddCommand(netmapChildCommands...)

	initCommonFlags(getEpochCmd)
	initCommonFlags(netInfoCmd)

	initCommonFlags(localNodeInfoCmd)
	localNodeInfoCmd.Flags().BoolVar(&nodeInfoJSON, "json", false, "print node info in JSON format")

	for _, netmapCommand := range netmapChildCommands {
		flags := netmapCommand.Flags()

		flags.StringSliceVarP(&xHeaders, xHeadersKey, xHeadersShorthand, xHeadersDefault, xHeadersUsage)
		flags.Uint32P(ttl, ttlShorthand, ttlDefault, ttlUsage)
	}
}

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	Run: func(cmd *cobra.Command, args []string) {
		var prm internalclient.NetworkInfoPrm

		prepareAPIClient(cmd, &prm)

		res, err := internalclient.NetworkInfo(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		netInfo := res.NetworkInfo()

		cmd.Println(netInfo.CurrentEpoch())
	},
}

var localNodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get local node info",
	Long:  `Get local node info`,
	Run: func(cmd *cobra.Command, args []string) {
		var prm internalclient.NodeInfoPrm

		prepareAPIClient(cmd, &prm)

		res, err := internalclient.NodeInfo(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		prettyPrintNodeInfo(cmd, res.NodeInfo(), nodeInfoJSON)
	},
}

type netCfgWriter cobra.Command

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

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long:  "Get information about NeoFS network",
	Run: func(cmd *cobra.Command, args []string) {
		var prm internalclient.NetworkInfoPrm

		prepareAPIClient(cmd, &prm)

		res, err := internalclient.NetworkInfo(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		netInfo := res.NetworkInfo()

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)

		cmd.Printf("Time per block: %s\n", time.Duration(netInfo.MsPerBlock())*time.Millisecond)

		netCfg := netInfo.NetworkConfig()

		cmd.Println("NeoFS network configuration")

		err = wrapper.WriteConfig((*netCfgWriter)(cmd), func(f func(key []byte, val []byte) error) error {
			var err error

			netCfg.IterateParameters(func(prm *netmap.NetworkParameter) bool {
				err = f(prm.Key(), prm.Value())
				return err != nil
			})

			return err
		})
		exitOnErr(cmd, errf("read config: %w", err))
	},
}

func prettyPrintNodeInfo(cmd *cobra.Command, i *netmap.NodeInfo, jsonEncoding bool) {
	if jsonEncoding {
		printJSONMarshaler(cmd, i, "node info")
		return
	}

	cmd.Println("key:", hex.EncodeToString(i.PublicKey()))
	cmd.Println("state:", i.State())
	netmap.IterateAllAddresses(i, func(s string) {
		cmd.Println("address:", s)
	})

	for _, attribute := range i.Attributes() {
		cmd.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}
}

func prettyPrintNetmap(cmd *cobra.Command, nm *control.Netmap, jsonEncoding bool) {
	if jsonEncoding {
		printJSONMarshaler(cmd, nm, "netmap")
		return
	}

	cmd.Println("Epoch:", nm.GetEpoch())

	for i, node := range nm.GetNodes() {
		cmd.Printf("Node %d: %s %s %v\n", i+1,
			base58.Encode(node.GetPublicKey()),
			node.GetState(),
			node.GetAddresses(),
		)

		for _, attr := range node.GetAttributes() {
			cmd.Printf("\t%s: %s\n", attr.GetKey(), attr.GetValue())
		}
	}
}
