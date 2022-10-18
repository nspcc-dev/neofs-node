package storagegroup

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	"github.com/spf13/cobra"
)

// Cmd represents the storagegroup command.
var Cmd = &cobra.Command{
	Use:   "storagegroup",
	Short: "Operations with Storage Groups",
	Long:  `Operations with Storage Groups`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		commonflags.BindAPI(cmd)
	},
}

const (
	sgIDFlag  = "id"
	sgRawFlag = "raw"
)

func init() {
	storageGroupChildCommands := []*cobra.Command{
		sgPutCmd,
		sgGetCmd,
		sgListCmd,
		sgDelCmd,
	}

	Cmd.AddCommand(storageGroupChildCommands...)

	for _, sgCommand := range storageGroupChildCommands {
		objectCli.InitBearer(sgCommand)
		commonflags.InitAPI(sgCommand)
	}

	initSGPutCmd()
	initSGGetCmd()
	initSGListCmd()
	initSGDeleteCmd()
}
