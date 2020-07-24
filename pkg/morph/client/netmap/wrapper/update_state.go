package wrapper

import (
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// NodeState is a type of node states enumeration.
type NodeState int64

const (
	_ NodeState = iota

	// StateOffline is an offline node state value.
	StateOffline
)

// UpdatePeerState changes peer status through Netmap contract
// call.
func (w *Wrapper) UpdatePeerState(key []byte, state NodeState) error {
	args := contract.UpdateStateArgs{}
	args.SetPublicKey(key)
	args.SetState(int64(state))

	// invoke smart contract call
	//
	// Note: errors.Wrap returns nil on nil error arg.
	return errors.Wrap(
		w.client.UpdateState(args),
		"could not invoke smart contract",
	)
}
