package netmap

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

const (
	// AddNodeNotaryEvent is method name for netmap `addNode` operation
	// in `Netmap` contract. Is used as identificator for notary
	// node addition requests. It's the new method used instead of
	// AddPeerNotaryEvent on appropriate networks.
	AddNodeNotaryEvent = "addNode"
)

// Node2Info converts [netmaprpc.NetmapNode2] into [netmap.NodeInfo].
func Node2Info(n2 *netmaprpc.NetmapNode2) (netmap.NodeInfo, error) {
	var ni netmap.NodeInfo

	ni.SetNetworkEndpoints(n2.Addresses...)
	for k, v := range n2.Attributes {
		ni.SetAttribute(k, v)
	}
	ni.SetPublicKey(n2.Key.Bytes())
	switch {
	case n2.State.Cmp(netmaprpc.NodeStateOnline) == 0:
		ni.SetOnline()
	case n2.State.Cmp(netmaprpc.NodeStateMaintenance) == 0:
		ni.SetMaintenance()
	default:
		return netmap.NodeInfo{}, fmt.Errorf("unsupported node state %v", n2.State)
	}
	return ni, nil
}

// Candidate2Info converts [netmaprpc.NetmapCandidate] into [netmap.NodeInfo].
func Candidate2Info(c *netmaprpc.NetmapCandidate) (netmap.NodeInfo, error) {
	var ni netmap.NodeInfo

	ni.SetNetworkEndpoints(c.Addresses...)
	for k, v := range c.Attributes {
		ni.SetAttribute(k, v)
	}
	ni.SetPublicKey(c.Key.Bytes())
	switch {
	case c.State.Cmp(netmaprpc.NodeStateOnline) == 0:
		ni.SetOnline()
	case c.State.Cmp(netmaprpc.NodeStateMaintenance) == 0:
		ni.SetMaintenance()
	default:
		return netmap.NodeInfo{}, fmt.Errorf("unsupported node state %v", c.State)
	}
	return ni, nil
}

// ParseAddNodeNotary from NotaryEvent into netmap event structure.
func ParseAddNodeNotary(ne event.NotaryEvent) (event.Event, error) {
	var (
		ev AddNode
		v  = vm.New()
	)

	v.LoadScript(ne.ArgumentScript())
	err := v.Run()
	if err != nil {
		return nil, fmt.Errorf("VM failure: %w", err)
	}

	es := v.Estack()
	if es.Len() != 1 {
		return nil, errors.New("incorrect argument evaluation result for addNode")
	}
	err = ev.Node.FromStackItem(es.Pop().Item())
	if err != nil {
		return nil, err
	}
	ev.notaryRequest = ne.Raw()

	return ev, nil
}
