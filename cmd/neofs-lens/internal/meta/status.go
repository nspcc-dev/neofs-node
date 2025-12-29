package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var statCMD = &cobra.Command{
	Use:   "status",
	Short: "Object status information",
	Long:  `Get metabase's indexes related to an object.`,
	Args:  cobra.NoArgs,
	RunE:  statusFunc,
}

func init() {
	common.AddAddressFlag(statCMD, &vAddress)
	common.AddComponentPathFlag(statCMD, &vPath)
}

func statusFunc(cmd *cobra.Command, _ []string) error {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("invalid address argument: %w", err)
	}

	db, err := openMeta(true)
	if err != nil {
		return err
	}

	defer db.Close()

	res, err := db.ObjectStatus(addr)
	if err != nil {
		return fmt.Errorf("reading object status: %w", err)
	}

	cmd.Printf("Metabase version: %d\n", res.Version)
	cmd.Printf("Object state: %s\n", res.State)
	if len(res.HeaderIndex) > 0 {
		cmd.Println("Header field indexes:")
		for _, field := range res.HeaderIndex {
			cmd.Printf("\t%s: %x\n", field.K, field.V)
		}
	}

	return nil
}
