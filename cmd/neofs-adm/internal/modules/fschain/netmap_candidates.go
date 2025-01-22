package fschain

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func listNetmapCandidatesNodes(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return err
	}

	inv := invoker.New(c, nil)

	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}

	nmHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	useV2, _ := cmd.Flags().GetBool(nodeV2Flag)

	var nodes []netmapSDK.NodeInfo
	if !useV2 {
		res, err := inv.Call(nmHash, "netmapCandidates")
		if err != nil {
			return fmt.Errorf("can't fetch list of network config keys from the netmap contract: %w", err)
		}
		if res.State != "HALT" {
			return fmt.Errorf("netmap contract returned unexpected exception: %s", res.FaultException)
		}

		nm, err := netmap.DecodeNetMap(res.Stack)

		if err != nil {
			return fmt.Errorf("unable to decode netmap: %w", err)
		}
		nodes = nm.Nodes()
	} else {
		var (
			reader          = netmaprpc.NewReader(inv, nmHash)
			sess, iter, err = reader.ListCandidates()
		)
		if err != nil {
			return fmt.Errorf("can't list candidates: %w", err)
		}
		// Conversion can be avoided in future.
		nodes, err = netmap.CollectNodes(inv, sess, &iter)
		if err != nil {
			return fmt.Errorf("can't collect nodes: %w", err)
		}
	}
	for i := range nodes {
		cmdprinter.PrettyPrintNodeInfo(cmd, nodes[i], i, "", false)
	}
	return nil
}
