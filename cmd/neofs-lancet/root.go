package main

import (
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal/fstree"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal/meta"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal/object"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal/storage"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/util/gendoc"
	"github.com/spf13/cobra"
)

var command = &cobra.Command{
	Use:           "neofs-lancet",
	Short:         "NeoFS Storage Engine Lancet",
	Long:          `NeoFS Storage Engine Lancet provides tools to browse and change the contents of the NeoFS storage engine.`,
	RunE:          entryPoint,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func entryPoint(cmd *cobra.Command, _ []string) error {
	printVersion, _ := cmd.Flags().GetBool("version")
	if printVersion {
		cmd.Print(misc.BuildInfo("NeoFS Lancet"))

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
