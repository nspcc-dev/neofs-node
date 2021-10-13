package main

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/commands/inspect"
	cmdlist "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/commands/list"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/spf13/cobra"
)

var command = &cobra.Command{
	Use:          "neofs-lens",
	Short:        "NeoFS Storage Engine Lens",
	Long:         `NeoFS Storage Engine Lens provides tools to browse the contents of the NeoFS storage engine.`,
	RunE:         entryPoint,
	SilenceUsage: true,
}

func entryPoint(cmd *cobra.Command, _ []string) error {
	printVersion, err := cmd.Flags().GetBool("version")
	if err == nil && printVersion {
		fmt.Printf("Version: %s \nBuild: %s \nDebug: %s\n",
			misc.Version,
			misc.Build,
			misc.Debug,
		)
		return nil
	}

	return cmd.Usage()
}

func init() {
	command.AddCommand(
		cmdlist.Command,
		inspect.Command,
	)
}

func main() {
	err := command.Execute()
	if err != nil {
		os.Exit(1)
	}
}
