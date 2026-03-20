package storage

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal"
	"github.com/spf13/cobra"
)

var wcFlushCMD = &cobra.Command{
	Use:   "flush-write-caches",
	Short: "Flush write-caches",
	Long:  `Flush all write-caches from config. This will push all objects from write-caches to the corresponding blobstors.`,
	Args:  cobra.NoArgs,
	RunE:  wcFlushFunc,
}

func init() {
	common.AddConfigFileFlag(wcFlushCMD, &vConfig)
}

func wcFlushFunc(_ *cobra.Command, _ []string) error {
	storage, err := openEngine(false)
	if err != nil {
		return err
	}
	defer storage.Close()

	return storage.FlushWriteCaches()
}
