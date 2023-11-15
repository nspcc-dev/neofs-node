package innerring

import (
	"fmt"
	"math"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/deploy"
)

type neoFSSidechain struct {
	client *client.Client

	netmapContractMtx sync.RWMutex
	netmapContract    *netmap.Client
}

func newNeoFSSidechain(sidechainClient *client.Client) *neoFSSidechain {
	return &neoFSSidechain{
		client: sidechainClient,
	}
}

func (x *neoFSSidechain) CurrentState() (deploy.NeoFSState, error) {
	var res deploy.NeoFSState
	var err error

	x.netmapContractMtx.RLock()
	netmapContract := x.netmapContract
	x.netmapContractMtx.RUnlock()

	if netmapContract == nil {
		x.netmapContractMtx.Lock()

		if x.netmapContract == nil {
			netmapContractAddress, err := x.client.NNSContractAddress(client.NNSNetmapContractName)
			if err != nil {
				x.netmapContractMtx.Unlock()
				return res, fmt.Errorf("resolve address of the '%s' contract in NNS: %w", client.NNSNetmapContractName, err)
			}

			x.netmapContract, err = netmap.NewFromMorph(x.client, netmapContractAddress, 0)
			if err != nil {
				x.netmapContractMtx.Unlock()
				return res, fmt.Errorf("create Netmap contract client: %w", err)
			}
		}

		netmapContract = x.netmapContract

		x.netmapContractMtx.Unlock()
	}

	res.CurrentEpoch, err = netmapContract.Epoch()
	if err != nil {
		return res, fmt.Errorf("get current epoch from Netmap contract: %w", err)
	}

	res.CurrentEpochBlock, err = netmapContract.LastEpochBlock()
	if err != nil {
		return res, fmt.Errorf("get last epoch block from Netmap contract: %w", err)
	}

	epochDur, err := netmapContract.EpochDuration()
	if err != nil {
		return res, fmt.Errorf("get epoch duration from Netmap contract: %w", err)
	}

	if epochDur > math.MaxUint32 {
		return res, fmt.Errorf("epoch duration from Netmap contract overflows uint32: %d", epochDur)
	}

	res.EpochDuration = uint32(epochDur)

	return res, nil
}
