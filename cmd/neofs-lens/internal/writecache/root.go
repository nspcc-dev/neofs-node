package writecache

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/spf13/cobra"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
)

// Root contains `write-cache` command definition.
var Root = &cobra.Command{
	Use:   "write-cache",
	Short: "Operations with write-cache",
}

func init() {
	Root.AddCommand(listCMD)
	Root.AddCommand(getCMD)
}

// openWC opens and returns read-only writecache.Cache located in vPath.
func openWC() (writecache.Cache, error) {
	wc := writecache.New(writecache.WithPath(vPath))

	err := wc.Open(true)
	if err != nil {
		return nil, fmt.Errorf("could not open write-cache: %w", err)
	}

	return wc, nil
}
