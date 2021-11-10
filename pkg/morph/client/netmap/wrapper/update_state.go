package wrapper

import (
	"fmt"

	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// UpdatePeerState changes peer status through Netmap contract
// call.
func (w *Wrapper) UpdatePeerState(key []byte, state netmap.NodeState) error {
	args := contract.UpdateStateArgs{}
	args.SetPublicKey(key)
	args.SetState(int64(state.ToV2()))

	// invoke smart contract call
	if err := w.client.UpdateState(args); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
