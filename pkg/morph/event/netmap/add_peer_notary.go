package netmap

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
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
	const addNodeArgsCnt = 1
	var (
		ev  AddNode
		err error
	)

	args := ne.Params()
	if len(args) != addNodeArgsCnt {
		return nil, event.WrongNumberOfParameters(addNodeArgsCnt, len(args))
	}

	ev.Node, err = nodeFromPushedItem(args[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse netmap node from AppCall parameter: %w", err)
	}
	ev.notaryRequest = ne.Raw()

	return ev, nil
}

func nodeFromPushedItem(instr scparser.PushedItem) (netmaprpc.NetmapNode2, error) {
	var (
		res netmaprpc.NetmapNode2
		err error
	)

	fields := instr.List
	if len(fields) != 4 {
		return res, fmt.Errorf("wrong number of structure elements: expected 4, got %d", len(fields))
	}

	addrs := fields[0].List
	if addrs == nil {
		return res, errors.New("addresses: not an array")
	}
	res.Addresses = make([]string, len(addrs))
	for i, e := range addrs {
		res.Addresses[i], err = scparser.GetUTF8StringFromInstr(e.Instruction)
		if err != nil {
			return res, fmt.Errorf("address #%d: %w", i, err)
		}
	}

	attrs := fields[1].Map
	if attrs == nil {
		return res, errors.New("attributes: not a map")
	}
	res.Attributes = make(map[string]string, len(attrs))
	for i, attr := range attrs {
		k, err := scparser.GetUTF8StringFromInstr(attr.Key)
		if err != nil {
			return res, fmt.Errorf("attribute #%d key: %w", i, err)
		}
		v, err := scparser.GetUTF8StringFromInstr(attr.Value.Instruction)
		if err != nil {
			return res, fmt.Errorf("attribute #%d value: %w", i, err)
		}
		res.Attributes[k] = v
	}

	res.Key, err = scparser.GetPublicKeyFromInstr(fields[2].Instruction)
	if err != nil {
		return res, fmt.Errorf("key: %w", err)
	}

	res.State, err = scparser.GetBigIntFromInstr(fields[3].Instruction)
	if err != nil {
		return res, fmt.Errorf("state: %w", err)
	}

	return res, nil
}
