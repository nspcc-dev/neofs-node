package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	netmapcontract "github.com/nspcc-dev/neofs-contract/contracts/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// GetNetMapByEpoch calls "snapshotByEpoch" method with the given epoch and
// decodes netmap.NetMap from the response.
func (c *Client) GetNetMapByEpoch(epoch uint64) (*netmap.NetMap, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(epochSnapshotMethod)
	invokePrm.SetArgs(epoch)

	res, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			epochSnapshotMethod, err)
	}

	nm, err := DecodeNetMap(res)
	if err != nil {
		return nil, err
	}

	nm.SetEpoch(epoch)

	return nm, err
}

// GetCandidates calls "netmapCandidates" method and decodes []netmap.NodeInfo
// from the response.
func (c *Client) GetCandidates() ([]netmap.NodeInfo, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(netMapCandidatesMethod)

	res, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", netMapCandidatesMethod, err)
	}

	if len(res) > 0 {
		return decodeNodeList(res[0])
	}

	return nil, nil
}

// NetMap calls "netmap" method and decode netmap.NetMap from the response.
func (c *Client) NetMap() (*netmap.NetMap, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(netMapMethod)

	res, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			netMapMethod, err)
	}

	return DecodeNetMap(res)
}

func DecodeNetMap(resStack []stackitem.Item) (*netmap.NetMap, error) {
	var nm netmap.NetMap

	if len(resStack) > 0 {
		nodes, err := decodeNodeList(resStack[0])
		if err != nil {
			return nil, err
		}

		nm.SetNodes(nodes)
	}

	return &nm, nil
}

func decodeNodeList(itemNodes stackitem.Item) ([]netmap.NodeInfo, error) {
	itemArrNodes, err := client.ArrayFromStackItem(itemNodes)
	if err != nil {
		return nil, fmt.Errorf("decode item array of nodes from the response item: %w", err)
	}

	var nodes []netmap.NodeInfo

	if len(itemArrNodes) > 0 {
		nodes = make([]netmap.NodeInfo, len(itemArrNodes))

		for i := range itemArrNodes {
			err = decodeNodeInfo(&nodes[i], itemArrNodes[i])
			if err != nil {
				return nil, fmt.Errorf("decode node #%d: %w", i+1, err)
			}
		}
	}

	return nodes, nil
}

func decodeNodeInfo(dst *netmap.NodeInfo, itemNode stackitem.Item) error {
	nodeFields, err := client.ArrayFromStackItem(itemNode)
	if err != nil {
		return fmt.Errorf("decode item array of node fields: %w", err)
	}

	var node netmapcontract.Node

	if len(nodeFields) > 0 {
		node.BLOB, err = client.BytesFromStackItem(nodeFields[0])
		if err != nil {
			return fmt.Errorf("decode node info BLOB: %w", err)
		}
	}

	node.State = netmapcontract.NodeStateOnline

	if len(nodeFields) > 1 {
		state, err := client.IntFromStackItem(nodeFields[1])
		if err != nil {
			return fmt.Errorf("decode integer from 2nd item: %w", err)
		}

		node.State = netmapcontract.NodeState(state)
	}

	err = dst.Unmarshal(node.BLOB)
	if err != nil {
		return fmt.Errorf("decode node info: %w", err)
	}

	switch node.State {
	default:
		return fmt.Errorf("unsupported state %v", node.State)
	case netmapcontract.NodeStateOnline:
		dst.SetOnline()
	case netmapcontract.NodeStateOffline:
		dst.SetOffline()
	case netmapcontract.NodeStateMaintenance:
		dst.SetMaintenance()
	}

	return nil
}
