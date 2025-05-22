package netmap

import (
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// Check the new node and allow/reject adding it to netmap.
func (np *Processor) processAddNode(ev netmapEvent.AddNode) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new node notification")
		return
	}

	// check if notary transaction is valid, see #976
	originalRequest := ev.NotaryRequest()
	tx := originalRequest.MainTransaction
	ok, err := np.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
	if err != nil || !ok {
		np.log.Warn("non-halt notary transaction",
			zap.String("method", "netmap.AddNode"),
			zap.String("hash", tx.Hash().StringLE()),
			zap.Error(err))
		return
	}

	// unmarshal node info
	nodeInfo, err := netmapEvent.Node2Info(&ev.Node)
	if err != nil {
		// it will be nice to have tx id at event structure to log it
		np.log.Warn("can't parse network map candidate")
		return
	}
	var keyString = netmap.StringifyPublicKey(nodeInfo)

	// validate node info
	err = np.nodeValidator.Verify(nodeInfo)
	if err != nil {
		np.log.Warn("could not verify and update information about network map candidate",
			zap.String("key", keyString),
			zap.Error(err),
		)

		return
	}

	np.log.Info("approving network map candidate", zap.String("key", keyString))

	err = np.netmapClient.Morph().NotarySignAndInvokeTX(tx, false)
	if err != nil {
		np.log.Error("can't sign and send notary request calling netmap.AddPeer", zap.Error(err))
	}
}

// Process update peer notification by sending approval tx to the smart contract.
func (np *Processor) processUpdatePeer(ev netmapEvent.UpdatePeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore update peer notification")
		return
	}

	var err error

	nr := ev.NotaryRequest()
	err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction, false)

	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
	}
}
