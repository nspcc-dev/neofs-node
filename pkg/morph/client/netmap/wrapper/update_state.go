package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
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
