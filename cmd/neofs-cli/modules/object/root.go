package object

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

// Cmd represents the object command.
var Cmd = &cobra.Command{
	Use:   "object",
	Short: "Operations with Objects",
	Long:  `Operations with Objects`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		commonflags.BindAPI(cmd)
	},
}

func init() {
	objectRPCs := []*cobra.Command{
		objectPutCmd,
		objectDelCmd,
		objectGetCmd,
		objectSearchCmd,
		searchV2Cmd,
		objectHeadCmd,
		objectHashCmd,
		objectRangeCmd,
		objectLockCmd}

	Cmd.AddCommand(objectNodesCmd)
	Cmd.AddCommand(objectRPCs...)

	for _, objCommand := range objectRPCs {
		InitBearer(objCommand)
		commonflags.InitAPI(objCommand)
	}

	initObjectPutCmd()
	initObjectDeleteCmd()
	initObjectGetCmd()
	initObjectSearchCmd()
	initObjectHeadCmd()
	initObjectHashCmd()
	initObjectRangeCmd()
	initCommandObjectLock()
	initObjectNodesCmd()
}
