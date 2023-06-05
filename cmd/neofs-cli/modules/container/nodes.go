package container

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	containerAPI "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

var short bool

var containerNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Show nodes for container",
	Long:  "Show nodes taking part in a container at the current epoch.",
	Run: func(cmd *cobra.Command, args []string) {
		var cnr, pkey = getContainer(cmd)

		if pkey == nil {
			pkey = key.GetOrGenerate(cmd)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, pkey, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		resmap, err := internalclient.NetMapSnapshot(prm)
		common.ExitOnErr(cmd, "unable to get netmap snapshot", err)

		var id cid.ID
		containerAPI.CalculateID(&id, cnr)

		policy := cnr.PlacementPolicy()

		var cnrNodes [][]netmap.NodeInfo
		cnrNodes, err = resmap.NetMap().ContainerNodes(policy, id)
		common.ExitOnErr(cmd, "could not build container nodes for given container: %w", err)

		for i := range cnrNodes {
			cmd.Printf("Descriptor #%d, REP %d:\n", i+1, policy.ReplicaNumberByIndex(i))
			for j := range cnrNodes[i] {
				common.PrettyPrintNodeInfo(cmd, cnrNodes[i][j], j, "\t", short)
			}
		}
	},
}

func initContainerNodesCmd() {
	commonflags.Init(containerNodesCmd)

	flags := containerNodesCmd.Flags()
	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathFrom, fromFlag, "", fromFlagUsage)
	flags.BoolVar(&short, "short", false, "Shortens output of node info")
}
