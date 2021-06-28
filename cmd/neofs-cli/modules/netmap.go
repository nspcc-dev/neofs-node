package cmd

import (
	"context"
	"encoding/hex"
	"fmt"

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
}

func init() {
	rootCmd.AddCommand(netmapCmd)

	netmapCmd.AddCommand(
		getEpochCmd,
		localNodeInfoCmd,
		netInfoCmd,
	)

	localNodeInfoCmd.Flags().BoolVar(&nodeInfoJSON, "json", false, "print node info in JSON format")
}

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		cli, err := getSDKClient(key)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			cmd.PrintErrln(fmt.Errorf("rpc error: %w", err))
			return
		}

		cmd.Println(netInfo.CurrentEpoch())
	},
}

var localNodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get local node info",
	Long:  `Get local node info`,
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		cli, err := getSDKClient(key)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		nodeInfo, err := cli.EndpointInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			cmd.PrintErrln(fmt.Errorf("rpc error: %w", err))
			return
		}

		prettyPrintNodeInfo(cmd, nodeInfo.NodeInfo(), nodeInfoJSON)
	},
}

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long:  "Get information about NeoFS network",
	Run: func(cmd *cobra.Command, args []string) {
		key, err := getKey()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		cli, err := getSDKClient(key)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			cmd.PrintErrln(fmt.Errorf("rpc error: %w", err))
			return
		}

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
