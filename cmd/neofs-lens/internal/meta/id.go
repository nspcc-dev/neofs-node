package meta

import (
	"fmt"

	"github.com/mr-tron/base58"
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/spf13/cobra"
)

var idCMD = &cobra.Command{
	Use:   "id",
	Short: "Read shard ID the metabase is attached to",
	Args:  cobra.NoArgs,
	RunE:  idFunc,
}

func init() {
	common.AddComponentPathFlag(idCMD, &vPath)
}

func idFunc(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	idRaw, err := db.ReadShardID()
	if err != nil {
		return fmt.Errorf("metabase's `ReadShardID`: %w", err)
	}

	cmd.Println(base58.Encode(idRaw))

	return nil
}
