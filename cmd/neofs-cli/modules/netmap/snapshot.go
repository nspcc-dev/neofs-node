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

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Request current local snapshot of the network map",
	Long:  `Request current local snapshot of the network map`,
	Run: func(cmd *cobra.Command, args []string) {
		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, p, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		res, err := internalclient.NetMapSnapshot(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		prettyPrintNetMap(cmd, res.NetMap())
	},
}

func initSnapshotCmd() {
	commonflags.Init(snapshotCmd)
	commonflags.InitAPI(snapshotCmd)
}

func prettyPrintNetMap(cmd *cobra.Command, nm netmap.NetMap) {
	cmd.Println("Epoch:", nm.Epoch())

	nodes := nm.Nodes()
	for i := range nodes {
		var strState string

		switch {
		case nodes[i].IsOnline():
			strState = "ONLINE"
		case nodes[i].IsOffline():
			strState = "OFFLINE"
		}

		cmd.Printf("Node %d: %s %s ", i+1, hex.EncodeToString(nodes[i].PublicKey()), strState)

		netmap.IterateNetworkEndpoints(nodes[i], func(endpoint string) {
			cmd.Printf("%s ", endpoint)
		})

		cmd.Println()

		nodes[i].IterateAttributes(func(key, value string) {
			cmd.Printf("\t%s: %s\n", key, value)
		})
	}
}
