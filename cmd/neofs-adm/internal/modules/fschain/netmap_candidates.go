package fschain

import (
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
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

	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}

	nmHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	useV2, _ := cmd.Flags().GetBool(nodeV2Flag)

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
		for i, n := range nm.Nodes() {
			cmdprinter.PrettyPrintNodeInfo(cmd, n, i, "", false)
		}
		return nil
	}
	var (
		nodes  []netmaprpc.NetmapCandidate
		reader = netmaprpc.NewReader(inv, nmHash)
	)
	sess, iter, err := reader.ListCandidates()
	if err != nil {
		return fmt.Errorf("can't list candidates: %w", err)
	}
	defer func() {
		_ = inv.TerminateSession(sess)
	}()
	items, err := inv.TraverseIterator(sess, &iter, 0)
	for err == nil && len(items) > 0 {
		for _, itm := range items {
			var (
				c   netmaprpc.NetmapCandidate
				err = c.FromStackItem(itm)
			)
			if err != nil {
				return fmt.Errorf("can't decode candidate: %w", err)
			}
			nodes = append(nodes, c)
		}
		items, err = inv.TraverseIterator(sess, &iter, 0)
	}
	if err != nil {
		return fmt.Errorf("can't fetch candidates: %w", err)
	}
	for i, n := range nodes {
		var strState string

		switch {
		case n.State.Cmp(netmaprpc.NodeStateOnline) == 0:
			strState = "ONLINE"
		case n.State.Cmp(netmaprpc.NodeStateOffline) == 0:
			strState = "OFFLINE"
		case n.State.Cmp(netmaprpc.NodeStateMaintenance) == 0:
			strState = "MAINTENANCE"
		default:
			strState = "STATE_UNSUPPORTED"
		}

		cmd.Printf("Node %d: %s %s (last active: %d) ", i+1, hex.EncodeToString(n.Key.Bytes()), strState, n.LastActiveEpoch.Int64())

		for j := range n.Addresses {
			cmd.Printf("%s ", n.Addresses[j])
		}
		cmd.Println()

		for k, v := range n.Attributes {
			cmd.Printf("\t%s: %s\n", k, v)
		}
	}
	return nil
}
