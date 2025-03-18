package netmap

import (
	"encoding/hex"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// Process add peer notification by sanity check of new node
// local epoch timer.
func (np *Processor) processAddPeer(ev netmapEvent.AddPeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new peer notification")
		return
	}

	// check if notary transaction is valid, see #976
	originalRequest := ev.NotaryRequest()
	tx := originalRequest.MainTransaction
	ok, err := np.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
	if err != nil || !ok {
		np.log.Warn("non-halt notary transaction",
			zap.String("method", "netmap.AddPeer"),
			zap.String("hash", tx.Hash().StringLE()),
			zap.Error(err))
		return
	}

	// unmarshal node info
	var nodeInfo netmap.NodeInfo
	if err := nodeInfo.Unmarshal(ev.Node()); err != nil {
		// it will be nice to have tx id at event structure to log it
		np.log.Warn("can't parse network map candidate")
		return
	}

	np.validateCandidate(tx, nodeInfo, false)
}

func (np *Processor) validateCandidate(tx *transaction.Transaction, nodeInfo netmap.NodeInfo, v2 bool) {
	// validate node info
	var err = np.nodeValidator.Verify(nodeInfo)
	if err != nil {
		np.log.Warn("could not verify and update information about network map candidate",
			zap.String("public_key", hex.EncodeToString(nodeInfo.PublicKey())),
			zap.Error(err),
		)

		return
	}

	// sort attributes to make it consistent
	nodeInfo.SortAttributes()

	// marshal updated node info structure
	nodeInfoBinary := nodeInfo.Marshal()

	keyString := netmap.StringifyPublicKey(nodeInfo)

	updated := np.netmapSnapshot.touch(keyString, np.epochState.EpochCounter(), nodeInfoBinary)

	if v2 || updated {
		np.log.Info("approving network map candidate",
			zap.String("key", keyString))

		err = np.netmapClient.Morph().NotarySignAndInvokeTX(tx, false)
		if err != nil {
			np.log.Error("can't sign and send notary request calling netmap.AddPeer", zap.Error(err))
		}
	}
}

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
	np.validateCandidate(tx, nodeInfo, true)
}

// Process update peer notification by sending approval tx to the smart contract.
func (np *Processor) processUpdatePeer(ev netmapEvent.UpdatePeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore update peer notification")
		return
	}

	// flag node to remove from local view, so it can be re-bootstrapped
	// again before new epoch will tick
	np.netmapSnapshot.flag(ev.PublicKey().StringCompressed(), np.epochState.EpochCounter())

	var err error

	if ev.Maintenance() {
		err = np.nodeStateSettings.MaintenanceModeAllowed()
		if err != nil {
			np.log.Info("prevent switching node to maintenance state",
				zap.Error(err),
			)

			return
		}
	}

	nr := ev.NotaryRequest()
	err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction, false)

	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
	}
}
