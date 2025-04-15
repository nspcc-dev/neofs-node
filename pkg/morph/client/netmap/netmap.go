package netmap

import (
	"math/big"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// GetNetMapByEpoch calls "snapshotByEpoch" method with the given epoch and
// decodes netmap.NetMap from the response.
func (c *Client) GetNetMapByEpoch(epoch uint64) (*netmap.NetMap, error) {
	var (
		nm     *netmap.NetMap
		inv    = invoker.New(c.client.Morph(), nil)
		reader = netmaprpc.NewReader(inv, c.contract)
	)
	sess, iter, err := reader.ListNodes2(big.NewInt(int64(epoch)))
	if err != nil {
		return nil, err
	}
	nm, err = collectNetmap(inv, sess, &iter)
	if err != nil {
		return nil, err
	}

	nm.SetEpoch(epoch)

	return nm, err
}

// GetCandidates calls "netmapCandidates" method and decodes []netmap.NodeInfo
// from the response.
func (c *Client) GetCandidates() ([]netmap.NodeInfo, error) {
	var (
		inv             = invoker.New(c.client.Morph(), nil)
		reader          = netmaprpc.NewReader(inv, c.contract)
		sess, iter, err = reader.ListCandidates()
	)
	if err != nil {
		return nil, err
	}
	return CollectNodes(inv, sess, &iter, netmapEvent.Candidate2Info)
}

// NetMap calls "netmap" method (or listNodes for v2 nodes) and decodes
// netmap.NetMap from the response.
func (c *Client) NetMap() (*netmap.NetMap, error) {
	var (
		inv             = invoker.New(c.client.Morph(), nil)
		reader          = netmaprpc.NewReader(inv, c.contract)
		sess, iter, err = reader.ListNodes()
	)
	if err != nil {
		return nil, err
	}
	return collectNetmap(inv, sess, &iter)
}

func collectNetmap(inv *invoker.Invoker, sess uuid.UUID, iter *result.Iterator) (*netmap.NetMap, error) {
	nodes, err := CollectNodes(inv, sess, iter, netmapEvent.Node2Info)
	if err != nil {
		return nil, err
	}
	var nm = new(netmap.NetMap)
	if len(nodes) > 0 {
		nm.SetNodes(nodes)
	}
	return nm, nil
}

// CollectNodes gathers all node data from the provided iterator and closes it.
func CollectNodes[N any, P interface {
	*N
	stackitem.Convertible
}](inv *invoker.Invoker, sess uuid.UUID, iter *result.Iterator,
	converter func(P) (netmap.NodeInfo, error)) ([]netmap.NodeInfo, error) {
	var nodes []netmap.NodeInfo

	defer func() {
		_ = inv.TerminateSession(sess)
	}()
	items, err := inv.TraverseIterator(sess, iter, 0)
	for err == nil && len(items) > 0 {
		for _, itm := range items {
			var (
				n2  N
				ni  netmap.NodeInfo
				err = P(&n2).FromStackItem(itm)
			)
			if err != nil {
				return nil, err
			}
			ni, err = converter(P(&n2))
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, ni)
		}
		items, err = inv.TraverseIterator(sess, iter, 0)
	}
	if err != nil {
		return nil, err
	}
	return nodes, nil
}
