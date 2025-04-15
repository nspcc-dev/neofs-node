package object

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

const (
	shortFlag      = "short"
	shortFlagUsage = "Short node output info"
)

// object lock command.
var objectNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "Show nodes for an object",
	Long:  "Show nodes taking part in an object placement at the current epoch.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		var cnrID cid.ID
		err := readCID(cmd, &cnrID)
		if err != nil {
			return err
		}

		var oID oid.ID
		err = readOID(cmd, &oID)
		if err != nil {
			return err
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}

		var prmSnap internalclient.NetMapSnapshotPrm
		prmSnap.SetClient(cli)

		resmap, err := internalclient.NetMapSnapshot(ctx, prmSnap)
		if err != nil {
			return fmt.Errorf("could not get netmap snapshot: %w", err)
		}

		var prmCnr internalclient.GetContainerPrm
		prmCnr.SetClient(cli)
		prmCnr.SetContainer(cnrID)

		res, err := internalclient.GetContainer(ctx, prmCnr)
		if err != nil {
			return fmt.Errorf("could not get container: %w", err)
		}

		cnr := res.Container()
		policy := cnr.PlacementPolicy()

		var cnrNodes [][]netmap.NodeInfo
		cnrNodes, err = resmap.NetMap().ContainerNodes(policy, cnrID)
		if err != nil {
			return fmt.Errorf("could not build container nodes for the given container: %w", err)
		}

		placementNodes, err := resmap.NetMap().PlacementVectors(cnrNodes, oID)
		if err != nil {
			return fmt.Errorf("could not build placement nodes for the given container: %w", err)
		}

		short, _ := cmd.Flags().GetBool(shortFlag)

		for i := range placementNodes {
			cmd.Printf("Descriptor #%d, REP %d:\n", i+1, policy.ReplicaNumberByIndex(i))
			for j := range placementNodes[i] {
				cmdprinter.PrettyPrintNodeInfo(cmd, placementNodes[i][j], j, "\t", short)
			}
		}

		return nil
	},
}

func initObjectNodesCmd() {
	ff := objectNodesCmd.Flags()

	ff.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	ff.DurationP(commonflags.Timeout, commonflags.TimeoutShorthand, commonflags.TimeoutDefault, commonflags.TimeoutUsage)
	ff.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	ff.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	ff.Bool(shortFlag, false, shortFlagUsage)
}
