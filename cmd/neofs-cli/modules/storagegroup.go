package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// storagegroupCmd represents the storagegroup command
var storagegroupCmd = &cobra.Command{
	Use:   "storagegroup",
	Short: "Operations with Storage Groups",
	Long:  `Operations with Storage Groups`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("storagegroup called")
	},
}

func init() {
	rootCmd.AddCommand(storagegroupCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// storagegroupCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// storagegroupCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
