package morphsubnet

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// UserAllowedPrm groups parameters of UserAllowed method of Subnet contract.
type UserAllowedPrm struct {
	args [2]interface{}
}

// SetID sets identifier of the subnet in a binary NeoFS API protocol format.
func (x *UserAllowedPrm) SetID(id []byte) {
	x.args[0] = id
}

// SetClient sets owner ID of the client that is being checked in a binary NeoFS API protocol format.
func (x *UserAllowedPrm) SetClient(id []byte) {
	x.args[1] = id
}

// UserAllowedRes groups resulting values of UserAllowed method of Subnet contract.
type UserAllowedRes struct {
	result bool
}

// Allowed returns true iff the client is allowed to create containers in the subnet.
func (x UserAllowedRes) Allowed() bool {
	return x.result
}

// UserAllowed checks if the user has access to the subnetwork.
func (x *Client) UserAllowed(prm UserAllowedPrm) (*UserAllowedRes, error) {
	args := client.TestInvokePrm{}

	args.SetMethod("userAllowed")
	args.SetArgs(prm.args[:]...)

	res, err := x.client.TestInvoke(args)
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

	return &UserAllowedRes{
		result: result,
	}, nil
}

// ManageClientsPrm groups parameters of client management in Subnet contract.
//
// Zero value adds subnet client. Subnet, group and client ID must be specified via setters.
type ManageClientsPrm struct {
	// remove or add client
	rm bool

	args [3]interface{}
}

// SetRemove marks client to be removed. By default, client is added.
func (x *ManageClientsPrm) SetRemove() {
	x.rm = true
}

// SetSubnet sets identifier of the subnet in a binary NeoFS API protocol format.
func (x *ManageClientsPrm) SetSubnet(id []byte) {
	x.args[0] = id
}

// SetGroup sets identifier of the client group in a binary NeoFS API protocol format.
func (x *ManageClientsPrm) SetGroup(id []byte) {
	x.args[1] = id
}

// SetClient sets client's user ID in a binary NeoFS API protocol format.
func (x *ManageClientsPrm) SetClient(id []byte) {
	x.args[2] = id
}

// ManageClientsRes groups resulting values of client management methods of Subnet contract.
type ManageClientsRes struct{}

// ManageClients manages client list of the NeoFS subnet through Subnet contract calls.
func (x Client) ManageClients(prm ManageClientsPrm) (*ManageClientsRes, error) {
	var method string

	if prm.rm {
		method = "removeUser"
	} else {
		method = "addUser"
	}

	var prmInvoke client.InvokePrm

	prmInvoke.SetMethod(method)
	prmInvoke.SetArgs(prm.args[:]...)

	err := x.client.Invoke(prmInvoke)
	if err != nil {
		return nil, err
	}

	return new(ManageClientsRes), nil
}
