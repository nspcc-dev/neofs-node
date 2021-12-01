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
