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

const nodeInfoJSONFlag = commonflags.JSON

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

func prettyPrintNodeInfo(cmd *cobra.Command, i netmap.NodeInfo) {
	isJSON, _ := cmd.Flags().GetBool(nodeInfoJSONFlag)
	if isJSON {
		common.PrettyPrintJSON(cmd, i, "node info")
		return
	}

	cmd.Println("key:", hex.EncodeToString(i.PublicKey()))

	var stateWord string
	switch {
	default:
		stateWord = "<undefined>"
	case i.IsOnline():
		stateWord = "online"
	case i.IsOffline():
		stateWord = "offline"
	}

	cmd.Println("state:", stateWord)

	netmap.IterateNetworkEndpoints(i, func(s string) {
		cmd.Println("address:", s)
	})

	i.IterateAttributes(func(key, value string) {
		cmd.Printf("attribute: %s=%s\n", key, value)
	})
}
