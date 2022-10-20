package tree

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "tree",
	Short: "Operations with the Tree service",
}

func init() {
	Cmd.AddCommand(addCmd)
	Cmd.AddCommand(getByPathCmd)
	Cmd.AddCommand(addByPathCmd)
	Cmd.AddCommand(listCmd)

	initAddCmd()
	initGetByPathCmd()
	initAddByPathCmd()
	initListCmd()
}

const (
	containerIDFlagKey = "cid"
	treeIDFlagKey      = "tid"
	parentIDFlagKey    = "pid"

	metaFlagKey = "meta"

	pathFlagKey          = "path"
	pathAttributeFlagKey = "pattr"

	latestOnlyFlagKey = "latest"
)

func initCTID(cmd *cobra.Command) {
	ff := cmd.Flags()

	ff.String(containerIDFlagKey, "", "Container ID")
	_ = cmd.MarkFlagRequired(containerIDFlagKey)

	ff.String(treeIDFlagKey, "", "Tree ID")
	_ = cmd.MarkFlagRequired(treeIDFlagKey)
}
