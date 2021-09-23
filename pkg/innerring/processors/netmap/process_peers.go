package netmap

import (
	"encoding/hex"
	"sort"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"go.uber.org/zap"
)

// Process add peer notification by sanity check of new node
// local epoch timer.
func (np *Processor) processAddPeer(ev netmapEvent.AddPeer) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new peer notification")
		return
	}

	// unmarshal node info
	nodeInfo := netmap.NewNodeInfo()
	if err := nodeInfo.Unmarshal(ev.Node()); err != nil {
		// it will be nice to have tx id at event structure to log it
		np.log.Warn("can't parse network map candidate")
		return
	}

	// validate and update node info
	err := np.nodeValidator.VerifyAndUpdate(nodeInfo)
	if err != nil {
		np.log.Warn("could not verify and update information about network map candidate",
			zap.String("error", err.Error()),
		)

		return
	}

	// sort attributes to make it consistent
	a := nodeInfo.Attributes()
	sort.Slice(a, func(i, j int) bool {
		switch strings.Compare(a[i].Key(), a[j].Key()) {
		case -1:
			return true
		case 1:
			return false
		default:
			return a[i].Value() < a[j].Value()
		}
	})
	nodeInfo.SetAttributes(a...)

	keyString := hex.EncodeToString(nodeInfo.PublicKey())

	exists := np.netmapSnapshot.touch(keyString, np.epochState.EpochCounter())
	if !exists {
		np.log.Info("approving network map candidate",
			zap.String("key", keyString))

		if nr := ev.NotaryRequest(); nr != nil {
			// notary event case

			var nodeInfoBinary []byte

			nodeInfoBinary, err = nodeInfo.Marshal()
			if err != nil {
				np.log.Warn("could not marshal updated network map candidate",
					zap.String("error", err.Error()),
				)

				return
			}

			// create new notary request with the original nonce
			err = np.netmapClient.Morph().NotaryInvoke(
				np.netmapContract,
				0,
				nr.MainTransaction.Nonce,
				netmapEvent.AddPeerNotaryEvent,
				nodeInfoBinary,
			)
		} else {
			// notification event case
			err = np.netmapClient.AddPeer(nodeInfo)
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

	// better use unified enum from neofs-api-go/v2/netmap package
	if ev.Status() != netmap.NodeStateOffline {
		np.log.Warn("node proposes unknown state",
			zap.String("key", hex.EncodeToString(ev.PublicKey().Bytes())),
			zap.Stringer("status", ev.Status()),
		)
		return
	}

	// flag node to remove from local view, so it can be re-bootstrapped
	// again before new epoch will tick
	np.netmapSnapshot.flag(hex.EncodeToString(ev.PublicKey().Bytes()))

	var err error

	if nr := ev.NotaryRequest(); nr != nil {
		err = np.netmapClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		err = np.netmapClient.UpdatePeerState(ev.PublicKey().Bytes(), ev.Status())
	}
	if err != nil {
		np.log.Error("can't invoke netmap.UpdatePeer", zap.Error(err))
	}
}
