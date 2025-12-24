package container

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

// Cmd represents the container command.
var Cmd = &cobra.Command{
	Use:   "container",
	Short: "Operations with containers",
	Long:  "Operations with containers",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		commonflags.BindAPI(cmd)
	},
}

func init() {
	containerChildCommand := []*cobra.Command{
		listContainersCmd,
		createContainerCmd,
		deleteContainerCmd,
		listContainerObjectsCmd,
		getContainerInfoCmd,
		getExtendedACLCmd,
		setExtendedACLCmd,
		containerNodesCmd,
		setAttributeCmd,
		removeAttributeCmd,
	}

	Cmd.AddCommand(containerChildCommand...)

	initContainerListContainersCmd()
	initContainerCreateCmd()
	initContainerDeleteCmd()
	initContainerListObjectsCmd()
	initContainerInfoCmd()
	initContainerGetEACLCmd()
	initContainerSetEACLCmd()
	initContainerNodesCmd()
	initSetAttributeCmd()
	initRemoveAttributeCmd()

	for _, containerCommand := range containerChildCommand {
		commonflags.InitAPI(containerCommand)
	}

	for _, el := range []struct {
		cmd  *cobra.Command
		verb string
	}{
		{createContainerCmd, "PUT"},
		{deleteContainerCmd, "DELETE"},
		{setExtendedACLCmd, "SETEACL"},
		{setAttributeCmd, "SETATTRIBUTE"},
		{removeAttributeCmd, "REMOVEATTRIBUTE"},
	} {
		commonflags.InitSession(el.cmd, "container "+el.verb)
	}
}
