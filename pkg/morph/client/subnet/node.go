package morphsubnet

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// NodeAllowedPrm groups parameters of NodeAllowed method of Subnet contract.
type NodeAllowedPrm struct {
	cliPrm client.TestInvokePrm

	args [2]interface{}
}

// SetID sets identifier of the subnet of the node in a binary NeoFS API protocol format.
func (x *NodeAllowedPrm) SetID(id []byte) {
	x.args[0] = id
}

// SetNode sets public key of the node that is being checked.
func (x *NodeAllowedPrm) SetNode(id []byte) {
	x.args[1] = id
}

// NodeAllowedRes groups resulting values of NodeAllowed method of Subnet contract.
type NodeAllowedRes struct {
	result bool
}

// Allowed returns true iff the node is allowed to enter the subnet.
func (x NodeAllowedRes) Allowed() bool {
	return x.result
}

// NodeAllowed checks if the node is included in the subnetwork.
func (x *Client) NodeAllowed(prm NodeAllowedPrm) (*NodeAllowedRes, error) {
	prm.cliPrm.SetMethod("nodeAllowed")
	prm.cliPrm.SetArgs(prm.args[:]...)

	res, err := x.client.TestInvoke(prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("could not make test invoke: %w", err)
	}

	if len(res) == 0 {
		return nil, errEmptyResponse
	}

	result, err := client.BoolFromStackItem(res[0])
	if err != nil {
		return nil, err
	}

	return &NodeAllowedRes{
		result: result,
	}, nil
}
