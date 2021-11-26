package morphsubnet

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutPrm groups parameters of Put method of Subnet contract.
type PutPrm struct {
	cliPrm client.InvokePrm

	args [3]interface{}
}

// SetTxHash sets hash of the transaction which spawned the notification.
// Ignore this parameter for new requests.
func (x *PutPrm) SetTxHash(hash util.Uint256) {
	x.cliPrm.SetHash(hash)
}

// SetID sets identifier of the created subnet in a binary NeoFS API protocol format.
func (x *PutPrm) SetID(id []byte) {
	x.args[0] = id
}

// SetOwner sets identifier of the subnet owner in a binary NeoFS API protocol format.
func (x *PutPrm) SetOwner(id []byte) {
	x.args[1] = id
}

// SetInfo sets information about the created subnet in a binary NeoFS API protocol format.
func (x *PutPrm) SetInfo(id []byte) {
	x.args[2] = id
}

// PutRes groups resulting values of Put method of Subnet contract.
type PutRes struct{}

// Put creates subnet though the call of the corresponding method of the Subnet contract.
func (x Client) Put(prm PutPrm) (*PutRes, error) {
	prm.cliPrm.SetMethod("put")
	prm.cliPrm.SetArgs(prm.args[:]...)

	err := x.client.Invoke(prm.cliPrm)
	if err != nil {
		return nil, err
	}

	return new(PutRes), nil
}
