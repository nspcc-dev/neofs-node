package cmd

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
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
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("netmap called")
	},
}

func init() {
	rootCmd.AddCommand(netmapCmd)

	netmapCmd.AddCommand(
		getEpochCmd,
		localNodeInfoCmd,
		snapshotCmd,
		netInfoCmd,
	)

	localNodeInfoCmd.Flags().BoolVar(&nodeInfoJSON, "json", false, "print node info in JSON format")

	snapshotCmd.Flags().BoolVar(&netmapSnapshotJSON, "json", false,
		"print netmap structure in JSON format")
}

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	RunE: func(cmd *cobra.Command, args []string) error {
		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		fmt.Println(netInfo.CurrentEpoch())

		return nil
	},
}

var localNodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get local node info",
	Long:  `Get local node info`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		nodeInfo, err := cli.EndpointInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		prettyPrintNodeInfo(nodeInfo.NodeInfo(), nodeInfoJSON)

		return nil
	},
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Get network map snapshot",
	Long:  "Get network map snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		key, err := getKey()
		if err != nil {
			return err
		}

		req := new(control.NetmapSnapshotRequest)
		req.SetBody(new(control.NetmapSnapshotRequest_Body))

		if err := controlSvc.SignMessage(key, req); err != nil {
			return err
		}

		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		resp, err := control.NetmapSnapshot(cli.Raw(), req)
		if err != nil {
			return err
		}

		sign := resp.GetSignature()

		if err := signature.VerifyDataWithSource(resp, func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		}); err != nil {
			return err
		}

		prettyPrintNetmap(resp.GetBody().GetNetmap(), netmapSnapshotJSON)

		return nil
	},
}

var netInfoCmd = &cobra.Command{
	Use:   "netinfo",
	Short: "Get information about NeoFS network",
	Long:  "Get information about NeoFS network",
	RunE: func(cmd *cobra.Command, args []string) error {
		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		netInfo, err := cli.NetworkInfo(context.Background(), globalCallOptions()...)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		cmd.Printf("Epoch: %d\n", netInfo.CurrentEpoch())

		magic := netInfo.MagicNumber()
		cmd.Printf("Network magic: [%s] %d\n", netmode.Magic(magic), magic)

		return nil
	},
}

func prettyPrintNodeInfo(i *netmap.NodeInfo, jsonEncoding bool) {
	if jsonEncoding {
		printJSONMarshaler(i, "node info")
		return
	}

	fmt.Println("key:", hex.EncodeToString(i.PublicKey()))
	fmt.Println("address:", i.Address())
	fmt.Println("state:", i.State())

	for _, attribute := range i.Attributes() {
		fmt.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}
}

func prettyPrintNetmap(nm *control.Netmap, jsonEncoding bool) {
	if jsonEncoding {
		printJSONMarshaler(nm, "netmap")
		return
	}

	fmt.Println("Epoch:", nm.GetEpoch())

	for i, node := range nm.GetNodes() {
		fmt.Printf("Node %d: %s %s %s\n", i+1,
			base58.Encode(node.GetPublicKey()),
			node.GetAddress(),
			node.GetState(),
		)

		for _, attr := range node.GetAttributes() {
			fmt.Printf("\t%s: %s\n", attr.GetKey(), attr.GetValue())
		}
	}
}
