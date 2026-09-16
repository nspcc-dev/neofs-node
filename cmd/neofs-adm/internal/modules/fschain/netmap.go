package fschain

import (
	"bytes"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/n3util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func getNetmap(cmd *cobra.Command, _ []string) error {
	publicKey, err := netmapPublicKey(cmd)
	if err != nil {
		return err
	}

	c, err := n3util.GetN3Client(viper.GetViper())
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

	reader := netmaprpc.NewReader(inv, nmHash)
	nodes, err := fetchNetmapNodes(cmd, inv, reader)
	if err != nil {
		return err
	}

	printNetmapHeader(cmd)
	printed := 0
	for _, node := range nodes {
		if printNetmapNode(cmd, printed+1, node, publicKey) {
			printed++
		}
	}
	if publicKey != nil && printed == 0 {
		cmd.Printf("Node with public key %s is not present in the network map.\n", hex.EncodeToString(publicKey.Bytes()))
	}
	cmd.Printf("Nodes: %d\n", printed)

	return nil
}

func fetchNetmapNodes(cmd *cobra.Command, inv *invoker.Invoker, reader *netmaprpc.ContractReader) ([]netmaprpc.NetmapNode2, error) {
	if cmd.Flags().Changed(netmapVersionFlag) {
		version, err := cmd.Flags().GetUint64(netmapVersionFlag)
		if err != nil {
			return nil, fmt.Errorf("read %s flag: %w", netmapVersionFlag, err)
		}
		sess, iter, err := reader.ListNodesVersion(bigIntFromUint64(version))
		if err != nil {
			return nil, fmt.Errorf("can't list nodes of netmap version %d: %w", version, err)
		}
		return collectNetmapNodes(inv, sess, &iter)
	}
	if cmd.Flags().Changed(netmapEpochFlag) {
		epoch, err := cmd.Flags().GetUint64(netmapEpochFlag)
		if err != nil {
			return nil, fmt.Errorf("read %s flag: %w", netmapEpochFlag, err)
		}
		sess, iter, err := reader.ListNodes2(bigIntFromUint64(epoch))
		if err != nil {
			return nil, fmt.Errorf("can't list nodes of netmap epoch %d: %w", epoch, err)
		}
		return collectNetmapNodes(inv, sess, &iter)
	}

	sess, iter, err := reader.ListNodes()
	if err != nil {
		return nil, fmt.Errorf("can't list current netmap nodes: %w", err)
	}
	return collectNetmapNodes(inv, sess, &iter)
}

func collectNetmapNodes(inv *invoker.Invoker, sess uuid.UUID, iter *result.Iterator) ([]netmaprpc.NetmapNode2, error) {
	defer func() {
		_ = inv.TerminateSession(sess)
	}()

	var nodes []netmaprpc.NetmapNode2
	items, err := inv.TraverseIterator(sess, iter, 0)
	for err == nil && len(items) > 0 {
		for _, item := range items {
			var node netmaprpc.NetmapNode2
			if err := node.FromStackItem(item); err != nil {
				return nil, fmt.Errorf("can't decode netmap node: %w", err)
			}
			nodes = append(nodes, node)
		}
		items, err = inv.TraverseIterator(sess, iter, 0)
	}
	if err != nil {
		return nil, fmt.Errorf("can't fetch netmap nodes: %w", err)
	}
	return nodes, nil
}

func printNetmapHeader(cmd *cobra.Command) {
	if cmd.Flags().Changed(netmapVersionFlag) {
		version, _ := cmd.Flags().GetUint64(netmapVersionFlag)
		cmd.Printf("Network map version %d:\n", version)
	} else if cmd.Flags().Changed(netmapEpochFlag) {
		epoch, _ := cmd.Flags().GetUint64(netmapEpochFlag)
		cmd.Printf("Network map at epoch %d:\n", epoch)
	} else {
		cmd.Println("Current network map:")
	}
}

func netmapPublicKey(cmd *cobra.Command) (*keys.PublicKey, error) {
	value, err := cmd.Flags().GetString(netmapPublicKeyFlag)
	if err != nil {
		return nil, fmt.Errorf("read %s flag: %w", netmapPublicKeyFlag, err)
	}
	if value == "" {
		return nil, nil
	}

	b, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode public key: %w", err)
	}
	key, err := keys.NewPublicKeyFromBytes(b, elliptic.P256())
	if err != nil {
		return nil, fmt.Errorf("invalid public key: %w", err)
	}
	return key, nil
}

func printNetmapNode(cmd *cobra.Command, i int, n netmaprpc.NetmapNode2, publicKey *keys.PublicKey) bool {
	if publicKey != nil && !bytes.Equal(n.Key.Bytes(), publicKey.Bytes()) {
		return false
	}

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

	cmd.Printf("Node %d: %s %s", i, hex.EncodeToString(n.Key.Bytes()), strState)
	for _, address := range n.Addresses {
		cmd.Printf(" %s", address)
	}
	cmd.Println()

	attributes := make([]string, 0, len(n.Attributes))
	for key := range n.Attributes {
		attributes = append(attributes, key)
	}
	sort.Strings(attributes)
	for _, key := range attributes {
		cmd.Printf("\t%s: %s\n", key, n.Attributes[key])
	}

	return true
}

func bigIntFromUint64(v uint64) *big.Int {
	return new(big.Int).SetUint64(v)
}
