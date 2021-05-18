package wrapper

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
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

	if err := w.client.AddPeer(args); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
