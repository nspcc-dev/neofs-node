package writecache

import (
	"fmt"
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var listCMD = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in a write-cache.`,
	Args:  cobra.NoArgs,
	RunE:  listFunc,
}

func init() {
	common.AddComponentPathFlag(listCMD, &vPath)
}

func listFunc(cmd *cobra.Command, _ []string) error {
	// other targets can be supported
	w := cmd.OutOrStderr()

	wAddr := func(addr oid.Address, _ []byte) error {
		_, err := io.WriteString(w, fmt.Sprintf("%s\n", addr))
		return err
	}

	wc, err := openWC()
	if err != nil {
		return err
	}
	defer wc.Close()

	err = wc.Iterate(wAddr, true)
	if err != nil {
		return fmt.Errorf("write-cache iterator failure: %w", err)
	}
	return nil
}
