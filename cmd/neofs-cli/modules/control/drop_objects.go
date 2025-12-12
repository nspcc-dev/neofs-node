package control

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

const dropObjectsFlag = "objects"

var dropObjectsCmd = &cobra.Command{
	Use:   "drop-objects",
	Short: "Drop objects from the node's local storage",
	Long:  "Drop objects from the node's local storage",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		pk, err := key.Get(cmd)
		if err != nil {
			return err
		}

		dropObjectsList, _ := cmd.Flags().GetStringSlice(dropObjectsFlag)
		binAddrList := make([][]byte, len(dropObjectsList))

		for i := range dropObjectsList {
			binAddrList[i] = []byte(dropObjectsList[i])
		}

		var req = &control.DropObjectsRequest{
			Body: &control.DropObjectsRequest_Body{
				AddressList: binAddrList,
			},
		}

		err = signRequest(pk, req)
		if err != nil {
			return err
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := getClient(ctx)
		if err != nil {
			return err
		}

		resp, err := cli.DropObjects(ctx, req)
		if err != nil {
			return err
		}

		err = verifyResponse(resp.GetSignature(), resp.GetBody())
		if err != nil {
			return err
		}

		cmd.Println("Objects were successfully removed.")
		return nil
	},
}

func initControlDropObjectsCmd() {
	initControlFlags(dropObjectsCmd)

	flags := dropObjectsCmd.Flags()
	flags.StringSliceP(dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)
}
