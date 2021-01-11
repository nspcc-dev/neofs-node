package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// UpdatePeerState changes peer status through Netmap contract
// call.
func (w *Wrapper) UpdatePeerState(key []byte, state netmap.NodeState) error {
	args := contract.UpdateStateArgs{}
	args.SetPublicKey(key)
	args.SetState(int64(state.ToV2()))

	// invoke smart contract call
	//
	// Note: errors.Wrap returns nil on nil error arg.
	return errors.Wrap(
		w.client.UpdateState(args),
		"could not invoke smart contract",
	)
}
