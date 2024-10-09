package meta

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/spf13/cobra"
)

var lastResyncEpochCMD = &cobra.Command{
	Use:   "last-resync-epoch",
	Short: "Read last epoch when metabase was resynchronized",
	Args:  cobra.NoArgs,
	RunE:  lastResyncEpochFunc,
}

func init() {
	common.AddComponentPathFlag(lastResyncEpochCMD, &vPath)
}

func lastResyncEpochFunc(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	epoch, err := db.ReadLastResyncEpoch()
	if err != nil {
		return err
	}
	cmd.Println(epoch)

	return nil
}
