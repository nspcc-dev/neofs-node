package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// UpdatePeerPrm groups parameters of UpdatePeerState operation.
type UpdatePeerPrm struct {
	key   []byte
	state netmap.NodeState

	client.InvokePrmOptional
}

// SetKey sets public key.
func (u *UpdatePeerPrm) SetKey(key []byte) {
	u.key = key
}

// SetState sets node state.
func (u *UpdatePeerPrm) SetState(state netmap.NodeState) {
	u.state = state
}

// UpdatePeerState changes peer status through Netmap contract
// call.
func (w *Wrapper) UpdatePeerState(prm UpdatePeerPrm) error {
	args := contract.UpdateStateArgs{}

	args.SetPublicKey(prm.key)
	args.SetState(int64(prm.state.ToV2()))
	args.InvokePrmOptional = prm.InvokePrmOptional

	// invoke smart contract call
	if err := w.client.UpdateState(args); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
