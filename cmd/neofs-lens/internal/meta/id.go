package meta

import (
	"github.com/mr-tron/base58"
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/spf13/cobra"
)

var idCMD = &cobra.Command{
	Use:   "id",
	Short: "Read shard ID the metabase is attached to",
	Args:  cobra.NoArgs,
	Run:   idFunc,
}

func init() {
	common.AddComponentPathFlag(idCMD, &vPath)
}

func idFunc(cmd *cobra.Command, _ []string) {
	db := openMeta(cmd, true)
	defer db.Close()

	idRaw, err := db.ReadShardID()
	common.ExitOnErr(cmd, common.Errf("metabase's `ReadShardID`: %w", err))

	cmd.Println(base58.Encode(idRaw))
}
