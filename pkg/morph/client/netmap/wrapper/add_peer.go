package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (w *Wrapper) AddPeer(nodeInfo netmap.Info) error {
	// prepare invocation arguments
	args := contract.AddPeerArgs{}

	info := contract.PeerInfo{}
	info.SetPublicKey(nodeInfo.PublicKey())
	info.SetAddress([]byte(nodeInfo.Address()))

	opts := nodeInfo.Options()
	binOpts := make([][]byte, 0, len(opts))

	for i := range opts {
		binOpts = append(binOpts, []byte(opts[i]))
	}

	info.SetOptions(binOpts)

	args.SetInfo(info)

	// invoke smart contract call
	//
	// Note: errors.Wrap returns nil on nil error arg.
	return errors.Wrap(
		w.client.AddPeer(args),
		"could not invoke smart contract",
	)
}
