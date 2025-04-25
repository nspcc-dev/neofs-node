package container

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

func (cp *Processor) processAnnounceLoad(e container.AnnounceLoad) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore announce load")
		return
	}

	err := cp.checkAnnounceLoad(e)
	if err != nil {
		cp.log.Error("announce load check failed",
			zap.Error(err),
		)

		return
	}

	nr := e.NotaryRequest()
	err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction, false)
	if err != nil {
		cp.log.Error("could not approve announce load",
			zap.Error(err),
		)
	}
}

func (cp *Processor) checkAnnounceLoad(e container.AnnounceLoad) error {
	binCnr := e.ContainerID()

	var idCnr cid.ID
	err := idCnr.Decode(binCnr)
	if err != nil || idCnr.IsZero() {
		return fmt.Errorf("invalid container ID: %w", err)
	}

	cnr, err := cp.cnrClient.Get(binCnr)
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	nm, err := cp.netState.NetMap()
	if err != nil {
		return fmt.Errorf("could not get netmap: %w", err)
	}

	ni, err := nm.ContainerNodes(cnr.PlacementPolicy(), idCnr)
	if err != nil {
		return fmt.Errorf("could not get container nodes: %w", err)
	}

	if !checkNodes(ni, e.Key()) {
		return fmt.Errorf("%s does not belong to container %s", e.Key(), idCnr.String())
	}

	return nil
}

func checkNodes(ni [][]netmapSDK.NodeInfo, key []byte) bool {
	for _, replicas := range ni {
		for _, node := range replicas {
			if bytes.Equal(node.PublicKey(), key) {
				return true
			}
		}
	}
	return false
}
