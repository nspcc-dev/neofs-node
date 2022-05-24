package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/spf13/cobra"
)

const dropObjectsFlag = "objects"

var dropObjectsCmd = &cobra.Command{
	Use:   "drop-objects",
	Short: "Drop objects from the node's local storage",
	Long:  "Drop objects from the node's local storage",
	Run: func(cmd *cobra.Command, args []string) {
		pk := key.Get(cmd)

		dropObjectsList, _ := cmd.Flags().GetStringSlice(dropObjectsFlag)
		binAddrList := make([][]byte, 0, len(dropObjectsList))

		for i := range dropObjectsList {
			a := addressSDK.NewAddress()

			err := a.Parse(dropObjectsList[i])
			if err != nil {
				common.ExitOnErr(cmd, "", fmt.Errorf("could not parse address #%d: %w", i, err))
			}

			binAddr, err := a.Marshal()
			common.ExitOnErr(cmd, "could not marshal the address: %w", err)

			binAddrList = append(binAddrList, binAddr)
		}

		body := new(control.DropObjectsRequest_Body)
		body.SetAddressList(binAddrList)

		req := new(control.DropObjectsRequest)
		req.SetBody(body)

		signRequest(cmd, pk, req)

		cli := getClient(cmd, pk)

		var resp *control.DropObjectsResponse
		var err error
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.DropObjects(client, req)
			return err
		})
		common.ExitOnErr(cmd, "rpc error: %w", err)

		verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

		cmd.Println("Objects were successfully marked to be removed.")
	},
}

func initControlDropObjectsCmd() {
	commonflags.InitWithoutRPC(dropObjectsCmd)

	flags := dropObjectsCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringSliceP(dropObjectsFlag, "o", nil,
		"List of object addresses to be removed in string format")

	_ = dropObjectsCmd.MarkFlagRequired(dropObjectsFlag)
}
