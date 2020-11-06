package cmd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/spf13/cobra"
)

var (
	nodeInfoJSON bool
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
	)

	localNodeInfoCmd.Flags().BoolVar(&nodeInfoJSON, "json", false, "print node info in JSON format")
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

		e, err := cli.Epoch(context.Background())
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		fmt.Println(e)

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

		nodeInfo, err := cli.EndpointInfo(context.Background())
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		prettyPrintNodeInfo(nodeInfo, nodeInfoJSON)

		return nil
	},
}

func prettyPrintNodeInfo(i *netmap.NodeInfo, jsonEncoding bool) {
	if jsonEncoding {
		data, err := netmap.NodeInfoToJSON(i)
		if err != nil {
			printVerbose("Can't convert container to json: %w", err)
			return
		}

		buf := new(bytes.Buffer)
		if err := json.Indent(buf, data, "", "  "); err != nil {
			printVerbose("Can't pretty print json: %w", err)
		}

		fmt.Println(buf)

		return
	}

	fmt.Println("key:", hex.EncodeToString(i.PublicKey()))
	fmt.Println("address:", i.Address())
	fmt.Println("state:", i.State())

	for _, attribute := range i.Attributes() {
		fmt.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}
}
