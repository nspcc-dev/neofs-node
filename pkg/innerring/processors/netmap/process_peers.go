package netmap

import (
	"encoding/hex"

	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
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

	// validate and update node info
	err = np.nodeValidator.VerifyAndUpdate(&nodeInfo)
	if err != nil {
		np.log.Warn("could not verify and update information about network map candidate",
			zap.String("error", err.Error()),
		)

		return
	}

	// sort attributes to make it consistent
	nodeInfo.SortAttributes()

	// marshal updated node info structure
	nodeInfoBinary := nodeInfo.Marshal()

	keyString := netmap.StringifyPublicKey(nodeInfo)

	updated := np.netmapSnapshot.touch(keyString, np.epochState.EpochCounter(), nodeInfoBinary)

	if updated {
		np.log.Info("approving network map candidate",
			zap.String("key", keyString))

		prm := netmapclient.AddPeerPrm{}
		prm.SetNodeInfo(nodeInfo)

		// In notary environments we call AddPeerIR method instead of AddPeer.
		// It differs from AddPeer only by name, so we can do this in the same form.
		// See https://github.com/nspcc-dev/neofs-contract/issues/154.
		const methodAddPeerNotary = "addPeerIR"

		// create new notary request with the original nonce
		err = np.netmapClient.Morph().NotaryInvoke(
			np.netmapClient.ContractAddress(),
			0,
			originalRequest.MainTransaction.Nonce,
			nil,
			methodAddPeerNotary,
			nodeInfoBinary,
		)

		if err != nil {
			np.log.Error("can't invoke netmap.AddPeer", zap.Error(err))
		}
	}
}

// Process update peer notification by sending approval tx to the smart contract.
func (np *Processor) processUpdatePeer(ev netmapEvent.UpdatePeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore update peer notification")
		return
	}

	// flag node to remove from local view, so it can be re-bootstrapped
	// again before new epoch will tick
	np.netmapSnapshot.flag(hex.EncodeToString(ev.PublicKey().Bytes()))

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
	err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)

	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
	}
}
