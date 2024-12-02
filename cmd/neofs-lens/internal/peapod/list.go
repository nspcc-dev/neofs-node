package peapod

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
	Long:  `List all objects stored in a Peapod.`,
	Args:  cobra.NoArgs,
	RunE:  listFunc,
}

func init() {
	common.AddComponentPathFlag(listCMD, &vPath)
}

func listFunc(cmd *cobra.Command, _ []string) error {
	// other targets can be supported
	w := cmd.OutOrStderr()

	wAddr := func(addr oid.Address) error {
		_, err := io.WriteString(w, fmt.Sprintf("%s\n", addr))
		return err
	}

	ppd, err := openPeapod()
	if err != nil {
		return err
	}
	defer ppd.Close()

	err = ppd.IterateAddresses(wAddr, false)
	if err != nil {
		return fmt.Errorf("Peapod iterator failure: %w", err)
	}
	return nil
}
