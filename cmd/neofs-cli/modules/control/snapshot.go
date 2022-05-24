package control

import (
	"github.com/mr-tron/base58"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

const (
	netmapSnapshotJSONFlag = "json"
)

var snapshotCmd = &cobra.Command{
	Use:   "netmap-snapshot",
	Short: "Get network map snapshot",
	Long:  "Get network map snapshot",
	Run: func(cmd *cobra.Command, args []string) {
		pk := key.Get(cmd)

		req := new(control.NetmapSnapshotRequest)
		req.SetBody(new(control.NetmapSnapshotRequest_Body))

		signRequest(cmd, pk, req)

		cli := getClient(cmd, pk)

		var resp *control.NetmapSnapshotResponse
		var err error
		err = cli.ExecRaw(func(client *rawclient.Client) error {
			resp, err = control.NetmapSnapshot(client, req)
			return err
		})
		common.ExitOnErr(cmd, "rpc error: %w", err)

		verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

		isJSON, _ := cmd.Flags().GetBool(netmapSnapshotJSONFlag)
		prettyPrintNetmap(cmd, resp.GetBody().GetNetmap(), isJSON)
	},
}

func initControlSnapshotCmd() {
	commonflags.InitWithoutRPC(snapshotCmd)

	flags := snapshotCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.Bool(netmapSnapshotJSONFlag, false, "print netmap structure in JSON format")
}

func prettyPrintNetmap(cmd *cobra.Command, nm *control.Netmap, jsonEncoding bool) {
	if jsonEncoding {
		common.PrettyPrintJSON(cmd, nm, "netmap")
		return
	}

	cmd.Println("Epoch:", nm.GetEpoch())

	for i, node := range nm.GetNodes() {
		cmd.Printf("Node %d: %s %s %v\n", i+1,
			base58.Encode(node.GetPublicKey()),
			node.GetState(),
			node.GetAddresses(),
		)

		for _, attr := range node.GetAttributes() {
			cmd.Printf("\t%s: %s\n", attr.GetKey(), attr.GetValue())
		}
	}
}
