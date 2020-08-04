package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// objectCmd represents the object command
var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "Operations with Objects",
	Long:  `Operations with Objects`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("object called")
	},
}

func init() {
	rootCmd.AddCommand(objectCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// objectCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// objectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
