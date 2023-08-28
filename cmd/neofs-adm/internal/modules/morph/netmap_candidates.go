package morph

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func listNetmapCandidatesNodes(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return err
	}

	inv := invoker.New(c, nil)

	nnsCs, err := c.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't get NNS contract info: %w", err)
	}

	nmHash, err := nnsResolveHash(inv, nnsCs.Hash, netmapContract+".neofs")
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

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
	nodes := nm.Nodes()
	for i := range nodes {
		cmdprinter.PrettyPrintNodeInfo(cmd, nodes[i], i, "", false)
	}
	return nil
}
