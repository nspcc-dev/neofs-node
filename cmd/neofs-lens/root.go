package main

import (
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/fstree"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/meta"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/object"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal/storage"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/gendoc"
	"github.com/spf13/cobra"
)

var command = &cobra.Command{
	Use:           "neofs-lens",
	Short:         "NeoFS Storage Engine Lens",
	Long:          `NeoFS Storage Engine Lens provides tools to browse the contents of the NeoFS storage engine.`,
	RunE:          entryPoint,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func entryPoint(cmd *cobra.Command, _ []string) error {
	printVersion, _ := cmd.Flags().GetBool("version")
	if printVersion {
		cmd.Print(misc.BuildInfo("NeoFS Lens"))

		return nil
	}

	return cmd.Usage()
}

func init() {
	// use stdout as default output for cmd.Print()
	command.SetOut(os.Stdout)
	command.Flags().Bool("version", false, "Application version")
	command.AddCommand(
		meta.Root,
		storage.Root,
		object.Root,
		fstree.Root,
		gendoc.Command(command),
	)
}

func main() {
	err := command.Execute()
	cmderr.ExitOnErr(err)
}
