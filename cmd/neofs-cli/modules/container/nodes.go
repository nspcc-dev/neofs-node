package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cnr, id, err := getContainer(ctx)
		if err != nil {
			return err
		}

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		nm, err := cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
		if err != nil {
			return fmt.Errorf("unable to get netmap snapshot: %w", err)
		}

		if id.IsZero() {
			id = cid.NewFromMarshalledContainer(cnr.Marshal())
		}

		policy := cnr.PlacementPolicy()

		var cnrNodes [][]netmap.NodeInfo
		cnrNodes, err = nm.ContainerNodes(policy, id)
		if err != nil {
			return fmt.Errorf("could not build container nodes for given container: %w", err)
		}

		repRuleNum := policy.NumberOfReplicas()
		for i := range repRuleNum {
			cmd.Printf("Descriptor #%d, REP %d:\n", i+1, policy.ReplicaNumberByIndex(i))
			for j := range cnrNodes[i] {
				cmdprinter.PrettyPrintNodeInfo(cmd, cnrNodes[i][j], j, "\t", short)
			}
		}

		cnrNodes = cnrNodes[repRuleNum:]

		ecRules := policy.ECRules()
		for i := range ecRules {
			cmd.Printf("EC descriptor #%d, EC %s:\n", i+1, iec.Rule{
				DataPartNum:   uint8(ecRules[i].DataPartNum()), // FIXME: do not cast to smaller integer
				ParityPartNum: uint8(ecRules[i].ParityPartNum()),
			})
			for j := range cnrNodes[i] {
				cmdprinter.PrettyPrintNodeInfo(cmd, cnrNodes[i][j], j, "\t", short)
			}
		}
		return nil
	},
}

func initContainerNodesCmd() {
	commonflags.Init(containerNodesCmd)

	flags := containerNodesCmd.Flags()
	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathFrom, fromFlag, "", fromFlagUsage)
	flags.BoolVar(&short, "short", false, "Shortens output of node info")
}
