package netmap

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"go.uber.org/zap"
)

// Process add peer notification by sanity check of new node
// local epoch timer.
func (np *Processor) processAddPeer(node []byte) {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore new peer notification")
		return
	}

	// unmarshal grpc (any transport) version of node info from API v2
	nodeInfo := new(netmap.NodeInfo)

	err := nodeInfo.Unmarshal(node)
	if err != nil {
		// it will be nice to have tx id at event structure to log it
		np.log.Warn("can't parse network map candidate")
		return
	}

	np.log.Info("approving network map candidate",
		zap.String("key", hex.EncodeToString(nodeInfo.PublicKey)),
	)

	err = invoke.ApprovePeer(np.morphClient, np.netmapContract, node)
	if err != nil {
		np.log.Error("can't invoke netmap.AddPeer", zap.Error(err))
	}
}

// Process new epoch tick by invoking new epoch method in network map contract.
func (np *Processor) processUpdatePeer(ev netmapEvent.UpdatePeer) {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore new epoch tick")
		return
	}

	// better use unified enum from neofs-api-go/v2/netmap package
	if ev.Status() != uint32(netmap.NodeInfo_OFFLINE) {
		np.log.Warn("node proposes unknown state",
			zap.String("key", hex.EncodeToString(ev.PublicKey().Bytes())),
			zap.Uint32("status", ev.Status()),
		)
		return
	}

	err := invoke.UpdatePeerState(np.morphClient, np.netmapContract,
		&invoke.UpdatePeerArgs{
			Key:    ev.PublicKey(),
			Status: ev.Status(),
		})
	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
	}
}
