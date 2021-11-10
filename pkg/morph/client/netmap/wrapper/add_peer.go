package wrapper

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

// AddPeerPrm groups parameters of AddPeer operation.
type AddPeerPrm struct {
	nodeInfo *netmap.NodeInfo

	client.InvokePrmOptional
}

// SetNodeInfo sets new peer NodeInfo.
func (a *AddPeerPrm) SetNodeInfo(nodeInfo *netmap.NodeInfo) {
	a.nodeInfo = nodeInfo
}

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (w *Wrapper) AddPeer(prm AddPeerPrm) error {
	if prm.nodeInfo == nil {
		return errors.New("nil node info")
	}

	rawNodeInfo, err := prm.nodeInfo.Marshal()
	if err != nil {
		return err
	}

	args := netmap.AddPeerArgs{}
	args.SetInfo(rawNodeInfo)
	args.InvokePrmOptional = prm.InvokePrmOptional

	if err := w.client.AddPeer(args); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
