package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// aclCmd represents the acl command
var aclCmd = &cobra.Command{
	Use:   "acl",
	Short: "Operations with Access Control Lists",
	Long:  `Operations with Access Control Lists`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("acl called")
	},
}

func init() {
	rootCmd.AddCommand(aclCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// aclCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// aclCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
