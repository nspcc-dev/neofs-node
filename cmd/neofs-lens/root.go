package main

import (
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/commands/inspect"
	cmdlist "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/commands/list"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/gendoc"
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
		cmd.Print(misc.BuildInfo("NeoFS Lens"))

		return nil
	}

	return cmd.Usage()
}

func init() {
	command.Flags().Bool("version", false, "application version")
	command.AddCommand(
		cmdlist.Command,
		inspect.Command,
		gendoc.Command(command),
	)
}

func main() {
	err := command.Execute()
	if err != nil {
		os.Exit(1)
	}
}
