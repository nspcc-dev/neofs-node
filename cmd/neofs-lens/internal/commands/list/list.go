package cmdlist

import (
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

const (
	flagFile       = "path"
	flagWriteCache = "writecache"
)

var (
	vPath       string
	vWriteCache bool
)

func init() {
	Command.Flags().StringVar(&vPath, flagFile, "",
		"Path to storage engine component",
	)
	_ = Command.MarkFlagFilename(flagFile)
	_ = Command.MarkFlagRequired(flagFile)

	Command.Flags().BoolVar(&vWriteCache, flagWriteCache, false,
		"Process write-cache",
	)
}

var Command = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in storage engine component.`,
	Run: func(cmd *cobra.Command, args []string) {
		// other targets can be supported
		w := cmd.OutOrStderr()

		wAddr := func(addr *object.Address) error {
			_, err := io.WriteString(w, addr.String()+"\n")
			return err
		}

		if vWriteCache {
			db, err := writecache.OpenDB(vPath, true)
			common.ExitOnErr(cmd, common.Errf("could not open write-cache db: %w", err))

			defer db.Close()

			err = writecache.IterateDB(db, wAddr)
			common.ExitOnErr(cmd, common.Errf("write-cache iterator failure: %w", err))

			return
		}

		blz := blobovnicza.New(
			blobovnicza.WithPath(vPath),
			blobovnicza.ReadOnly(),
		)

		common.ExitOnErr(cmd, blz.Open())

		defer blz.Close()

		err := blobovnicza.IterateAddresses(blz, wAddr)
		common.ExitOnErr(cmd, common.Errf("blobovnicza iterator failure: %w", err))
	},
}
