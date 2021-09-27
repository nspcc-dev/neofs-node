package cmd

import (
	"context"
	"encoding/hex"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
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
		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		cmd.Println(netInfo.CurrentEpoch())
	},
}

var localNodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get local node info",
	Long:  `Get local node info`,
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		nodeInfo, err := cli.EndpointInfo(context.Background(), globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		prettyPrintNodeInfo(cmd, nodeInfo.NodeInfo(), nodeInfoJSON)
	},
}

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long:  "Get information about NeoFS network",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		exitOnErr(cmd, err)

		cli, err := getSDKClient(key)
		exitOnErr(cmd, err)

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		exitOnErr(cmd, errf("rpc error: %w", err))

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)
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
