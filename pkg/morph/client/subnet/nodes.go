package morphsubnet

import "github.com/nspcc-dev/neofs-node/pkg/morph/client"

// ManageNodesPrm groups parameters of node management in Subnet contract.
//
// Zero value adds node to subnet. Subnet and node IDs must be specified via setters.
type ManageNodesPrm struct {
	// remove or add node
	rm bool

	args [2]interface{}
}

// SetRemove marks node to be removed. By default, node is added.
func (x *ManageNodesPrm) SetRemove() {
	x.rm = true
}

// SetSubnet sets identifier of the subnet in a binary NeoFS API protocol format.
func (x *ManageNodesPrm) SetSubnet(id []byte) {
	x.args[0] = id
}

// SetNode sets node's public key in a binary format.
func (x *ManageNodesPrm) SetNode(id []byte) {
	x.args[1] = id
}

// ManageNodesRes groups resulting values of node management methods of Subnet contract.
type ManageNodesRes struct{}

// ManageNodes manages node list of the NeoFS subnet through Subnet contract calls.
func (x Client) ManageNodes(prm ManageNodesPrm) (*ManageNodesRes, error) {
	var method string

	if prm.rm {
		method = "removeNode"
	} else {
		method = "addNode"
	}

	var prmInvoke client.InvokePrm

	prmInvoke.SetMethod(method)
	prmInvoke.SetArgs(prm.args[:]...)

	err := x.client.Invoke(prmInvoke)
	if err != nil {
		return nil, err
	}

	return new(ManageNodesRes), nil
}
