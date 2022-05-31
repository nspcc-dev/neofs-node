package netmap

import (
	"encoding/hex"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

const nodeInfoJSONFlag = "json"

var nodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get local node info",
	Long:  `Get local node info`,
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NodeInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NodeInfo(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		prettyPrintNodeInfo(cmd, res.NodeInfo())
	},
}

func initNodeInfoCmd() {
	commonflags.Init(nodeInfoCmd)
	commonflags.InitAPI(nodeInfoCmd)
	nodeInfoCmd.Flags().Bool(nodeInfoJSONFlag, false, "print node info in JSON format")
}

func prettyPrintNodeInfo(cmd *cobra.Command, i *netmap.NodeInfo) {
	isJSON, _ := cmd.Flags().GetBool(nodeInfoJSONFlag)
	if isJSON {
		common.PrettyPrintJSON(cmd, i, "node info")
		return
	}

	cmd.Println("key:", hex.EncodeToString(i.PublicKey()))
	cmd.Println("state:", i.State())
	netmap.IterateAllAddresses(i, func(s string) {
		cmd.Println("address:", s)
	})

	for _, attribute := range i.Attributes() {
		cmd.Printf("attribute: %s=%s\n", attribute.Key(), attribute.Value())
	}
}
