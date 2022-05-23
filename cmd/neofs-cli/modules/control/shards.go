package control

import (
	"github.com/spf13/cobra"
)

var shardsCmd = &cobra.Command{
	Use:   "shards",
	Short: "Operations with storage node's shards",
	Long:  "Operations with storage node's shards",
}

func initControlShardsCmd() {
	shardsCmd.AddCommand(listShardsCmd)
	shardsCmd.AddCommand(setShardModeCmd)
	shardsCmd.AddCommand(dumpShardCmd)
	shardsCmd.AddCommand(restoreShardCmd)

	initControlShardsListCmd()
	initControlSetShardModeCmd()
	initControlDumpShardCmd()
	initControlRestoreShardCmd()
}
