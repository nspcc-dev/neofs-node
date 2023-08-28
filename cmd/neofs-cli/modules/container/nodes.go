package container

import (
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
)

var short bool

var containerNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Show nodes for container",
	Long:  "Show nodes taking part in a container at the current epoch.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cnr := getContainer(ctx, cmd)

		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		resmap, err := internalclient.NetMapSnapshot(ctx, prm)
		common.ExitOnErr(cmd, "unable to get netmap snapshot", err)

		var id cid.ID
		cnr.CalculateID(&id)

		policy := cnr.PlacementPolicy()

		var cnrNodes [][]netmap.NodeInfo
		cnrNodes, err = resmap.NetMap().ContainerNodes(policy, id)
		common.ExitOnErr(cmd, "could not build container nodes for given container: %w", err)

		for i := range cnrNodes {
			cmd.Printf("Descriptor #%d, REP %d:\n", i+1, policy.ReplicaNumberByIndex(i))
			for j := range cnrNodes[i] {
				cmdprinter.PrettyPrintNodeInfo(cmd, cnrNodes[i][j], j, "\t", short)
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
