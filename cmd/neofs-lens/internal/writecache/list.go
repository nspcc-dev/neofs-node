package writecache

import (
	"fmt"
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var listCMD = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in a write-cache.`,
	Args:  cobra.NoArgs,
	Run:   listFunc,
}

func init() {
	common.AddComponentPathFlag(listCMD, &vPath)
}

func listFunc(cmd *cobra.Command, _ []string) {
	// other targets can be supported
	w := cmd.OutOrStderr()

	wAddr := func(addr oid.Address) error {
		_, err := io.WriteString(w, fmt.Sprintf("%s\n", addr))
		return err
	}

	db := openWC(cmd)
	defer db.Close()

	err := writecache.IterateDB(db, wAddr)
	common.ExitOnErr(cmd, common.Errf("write-cache iterator failure: %w", err))
}
