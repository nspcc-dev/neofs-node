package netmap

import (
	"bytes"
	"encoding/hex"

	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	subnetEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
)

// Process add peer notification by sanity check of new node
// local epoch timer.
func (np *Processor) processAddPeer(ev netmapEvent.AddPeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new peer notification")
		return
	}

	// check if notary transaction is valid, see #976
	if originalRequest := ev.NotaryRequest(); originalRequest != nil {
		tx := originalRequest.MainTransaction
		ok, err := np.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
		if err != nil || !ok {
			np.log.Warn("non-halt notary transaction",
				logger.FieldString("method", "netmap.AddPeer"),
				logger.FieldString("hash", tx.Hash().StringLE()),
				logger.FieldError(err))
			return
		}
	}

	// unmarshal node info
	var nodeInfo netmap.NodeInfo
	if err := nodeInfo.Unmarshal(ev.Node()); err != nil {
		// it will be nice to have tx id at event structure to log it
		np.log.Warn("can't parse network map candidate")
		return
	}

	// validate and update node info
	err := np.nodeValidator.VerifyAndUpdate(&nodeInfo)
	if err != nil {
		np.log.Warn("could not verify and update information about network map candidate",
			logger.FieldError(err),
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
			logger.FieldString("key", keyString))

		prm := netmapclient.AddPeerPrm{}
		prm.SetNodeInfo(nodeInfo)

		// In notary environments we call AddPeerIR method instead of AddPeer.
		// It differs from AddPeer only by name, so we can do this in the same form.
		// See https://github.com/nspcc-dev/neofs-contract/issues/154.
		const methodAddPeerNotary = "addPeerIR"

		if nr := ev.NotaryRequest(); nr != nil {
			// create new notary request with the original nonce
			err = np.netmapClient.Morph().NotaryInvoke(
				np.netmapClient.ContractAddress(),
				0,
				nr.MainTransaction.Nonce,
				nil,
				methodAddPeerNotary,
				nodeInfoBinary,
			)
		} else {
			// notification event case
			err = np.netmapClient.AddPeer(prm)
		}

		if err != nil {
			np.log.Error("can't invoke netmap.AddPeer", logger.FieldError(err))
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
				logger.FieldError(err),
			)

			return
		}
	}

	if nr := ev.NotaryRequest(); nr != nil {
		err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		prm := netmapclient.UpdatePeerPrm{}

		switch {
		case ev.Online():
			prm.SetOnline()
		case ev.Maintenance():
			prm.SetMaintenance()
		}

		prm.SetKey(ev.PublicKey().Bytes())

		err = np.netmapClient.UpdatePeerState(prm)
	}
	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", logger.FieldError(err))
	}
}

func (np *Processor) processRemoveSubnetNode(ev subnetEvent.RemoveNode) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore remove node from subnet notification")
		return
	}

	candidates, err := np.netmapClient.GetCandidates()
	if err != nil {
		np.log.Warn("could not get network map candidates",
			logger.FieldError(err),
		)
		return
	}

	rawSubnet := ev.SubnetworkID()
	var subnetToRemoveFrom subnetid.ID

	err = subnetToRemoveFrom.Unmarshal(rawSubnet)
	if err != nil {
		np.log.Warn("could not unmarshal subnet id",
			logger.FieldError(err),
		)
		return
	}

	if subnetid.IsZero(subnetToRemoveFrom) {
		np.log.Warn("got zero subnet in remove node notification")
		return
	}

	for i := range candidates {
		if !bytes.Equal(candidates[i].PublicKey(), ev.Node()) {
			continue
		}

		err = candidates[i].IterateSubnets(func(subNetID subnetid.ID) error {
			if subNetID.Equals(subnetToRemoveFrom) {
				return netmap.ErrRemoveSubnet
			}

			return nil
		})
		if err != nil {
			np.log.Warn("could not iterate over subnetworks of the node", logger.FieldError(err))
			np.log.Info("vote to remove node from netmap", logger.FieldString("key", hex.EncodeToString(ev.Node())))

			prm := netmapclient.UpdatePeerPrm{}
			prm.SetKey(ev.Node())
			prm.SetHash(ev.TxHash())

			err = np.netmapClient.UpdatePeerState(prm)
			if err != nil {
				np.log.Error("could not invoke netmap.UpdateState", logger.FieldError(err))
				return
			}
		} else {
			prm := netmapclient.AddPeerPrm{}
			prm.SetNodeInfo(candidates[i])
			prm.SetHash(ev.TxHash())

			err = np.netmapClient.AddPeer(prm)
			if err != nil {
				np.log.Error("could not invoke netmap.AddPeer", logger.FieldError(err))
				return
			}
		}

		break
	}
}
