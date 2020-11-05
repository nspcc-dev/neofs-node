package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
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
	)
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
