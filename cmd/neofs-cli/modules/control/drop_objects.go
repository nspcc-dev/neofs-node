package control

import (
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
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
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		pk, err := key.Get(cmd)
		if err != nil {
			return err
		}

		dropObjectsList, _ := cmd.Flags().GetStringSlice(dropObjectsFlag)
		binAddrList := make([][]byte, len(dropObjectsList))

		for i := range dropObjectsList {
			binAddrList[i] = []byte(dropObjectsList[i])
		}

		body := new(control.DropObjectsRequest_Body)
		body.SetAddressList(binAddrList)

		req := new(control.DropObjectsRequest)
		req.SetBody(body)

		err = signRequest(pk, req)
		if err != nil {
			return err
		}

		cli, err := getClient(ctx)
		if err != nil {
			return err
		}

		var resp *control.DropObjectsResponse
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.DropObjects(client, req)
			return err
		})
		if err != nil {
			return err
		}

		err = verifyResponse(resp.GetSignature(), resp.GetBody())
		if err != nil {
			return err
		}

		cmd.Println("Objects were successfully marked to be removed.")
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
