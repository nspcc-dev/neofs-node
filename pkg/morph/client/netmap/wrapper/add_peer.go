package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (w *Wrapper) AddPeer(nodeInfo *netmap.NodeInfo) error {
	if nodeInfo == nil {
		return errors.New("nil node info")
	}

	rawNodeInfo, err := nodeInfo.Marshal()
	if err != nil {
		return err
	}

	args := netmap.AddPeerArgs{}
	args.SetInfo(rawNodeInfo)

	return errors.Wrap(
		w.client.AddPeer(args),
		"could not invoke smart contract",
	)
}
