package cmd

import (
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

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// netmapCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// netmapCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
