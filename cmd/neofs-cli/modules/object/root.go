package object

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

// Cmd represents the object command
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
	objectChildCommands := []*cobra.Command{
		objectPutCmd,
		objectDelCmd,
		objectGetCmd,
		objectSearchCmd,
		objectHeadCmd,
		objectHashCmd,
		objectRangeCmd,
		objectLockCmd}

	Cmd.AddCommand(objectChildCommands...)

	for _, objCommand := range objectChildCommands {
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
}
