package netmap

import (
	"bytes"
	"encoding/hex"

	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	subnetEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
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
	if originalRequest := ev.NotaryRequest(); originalRequest != nil {
		tx := originalRequest.MainTransaction
		ok, err := np.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
		if err != nil || !ok {
			np.log.Warn("non-halt notary transaction",
				zap.String("method", "netmap.AddPeer"),
				zap.String("hash", tx.Hash().StringLE()),
				zap.Error(err))
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
			zap.String("error", err.Error()),
		)

		return
	}

	// sort attributes to make it consistent
	nodeInfo.SortAttributes()

	// marshal updated node info structure
	nodeInfoBinary := nodeInfo.Marshal()

	keyString := hex.EncodeToString(nodeInfo.PublicKey())

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

	if nr := ev.NotaryRequest(); nr != nil {
		err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		prm := netmapclient.UpdatePeerPrm{}

		if ev.Online() {
			prm.SetOnline()
		}
		prm.SetKey(ev.PublicKey().Bytes())

		err = np.netmapClient.UpdatePeerState(prm)
	}
	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
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
			zap.Error(err),
		)
		return
	}

	rawSubnet := ev.SubnetworkID()
	var subnetToRemoveFrom subnetid.ID

	err = subnetToRemoveFrom.Unmarshal(rawSubnet)
	if err != nil {
		np.log.Warn("could not unmarshal subnet id",
			zap.Error(err),
		)
		return
	}

	if subnetid.IsZero(subnetToRemoveFrom) {
		np.log.Warn("got zero subnet in remove node notification")
		return
	}

	candidateNodes := candidates.Nodes()

	for i := range candidateNodes {
		if !bytes.Equal(candidateNodes[i].PublicKey(), ev.Node()) {
			continue
		}

		err = candidateNodes[i].IterateSubnets(func(subNetID subnetid.ID) error {
			if subNetID.Equals(subnetToRemoveFrom) {
				return netmap.ErrRemoveSubnet
			}

			return nil
		})
		if err != nil {
			np.log.Warn("could not iterate over subnetworks of the node", zap.Error(err))
			np.log.Info("vote to remove node from netmap", zap.String("key", hex.EncodeToString(ev.Node())))

			prm := netmapclient.UpdatePeerPrm{}
			prm.SetKey(ev.Node())
			prm.SetHash(ev.TxHash())

			err = np.netmapClient.UpdatePeerState(prm)
			if err != nil {
				np.log.Error("could not invoke netmap.UpdateState", zap.Error(err))
				return
			}
		} else {
			prm := netmapclient.AddPeerPrm{}
			prm.SetNodeInfo(candidateNodes[i])
			prm.SetHash(ev.TxHash())

			err = np.netmapClient.AddPeer(prm)
			if err != nil {
				np.log.Error("could not invoke netmap.AddPeer", zap.Error(err))
				return
			}
		}

		break
	}
}
