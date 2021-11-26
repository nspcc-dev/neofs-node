package morphsubnet

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// GetPrm groups parameters of Get method of Subnet contract.
type GetPrm struct {
	cliPrm client.TestInvokePrm

	args [1]interface{}
}

// SetID sets identifier of the subnet to be read in a binary NeoFS API protocol format.
func (x *GetPrm) SetID(id []byte) {
	x.args[0] = id
}

// GetRes groups resulting values of Get method of Subnet contract.
type GetRes struct {
	info []byte
}

// Info returns information about the subnet in a binary format of NeoFS API protocol.
func (x GetRes) Info() []byte {
	return x.info
}

var errEmptyResponse = errors.New("empty response")

// Get reads the subnet through the call of the corresponding method of the Subnet contract.
func (x *Client) Get(prm GetPrm) (*GetRes, error) {
	prm.cliPrm.SetMethod("get")
	prm.cliPrm.SetArgs(prm.args[:]...)

	res, err := x.client.TestInvoke(prm.cliPrm)
	if err != nil {
		fmt.Println()
		return nil, err
	}

	if len(res) == 0 {
		return nil, errEmptyResponse
	}

	data, err := client.BytesFromStackItem(res[0])
	if err != nil {
		return nil, err
	}

	return &GetRes{
		info: data,
	}, nil
}
