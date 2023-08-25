package control

import (
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		pk := key.Get(cmd)

		dropObjectsList, _ := cmd.Flags().GetStringSlice(dropObjectsFlag)
		binAddrList := make([][]byte, len(dropObjectsList))

		for i := range dropObjectsList {
			binAddrList[i] = []byte(dropObjectsList[i])
		}

		body := new(control.DropObjectsRequest_Body)
		body.SetAddressList(binAddrList)

		req := new(control.DropObjectsRequest)
		req.SetBody(body)

		signRequest(cmd, pk, req)

		cli := getClient(ctx, cmd)

		var resp *control.DropObjectsResponse
		var err error
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.DropObjects(client, req)
			return err
		})
		common.ExitOnErr(cmd, "", err)

		verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

		cmd.Println("Objects were successfully marked to be removed.")
	},
}

func initControlDropObjectsCmd() {
	initControlFlags(dropObjectsCmd)

	flags := dropObjectsCmd.Flags()
	flags.StringSliceP(dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)
}
