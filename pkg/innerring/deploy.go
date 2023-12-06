package innerring

import (
	"fmt"
	"math"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/deploy"
)

type neoFSSidechain struct {
	client *client.Client

	netmapContractMtx sync.RWMutex
	netmapContract    *netmap.Client

	*rpcclient.WSClient

	subsMtx sync.Mutex
	subs    []string
}

// cancels all active subscriptions. Must not be called concurrently with
// subscribe methods.
func (x *neoFSSidechain) cancelSubs() {
	for i := range x.subs {
		_ = x.WSClient.Unsubscribe(x.subs[i])
	}
}

// SubscribeToNewBlocks implements [deploy.Blockchain] interface.
func (x *neoFSSidechain) SubscribeToNewBlocks() (<-chan *block.Block, error) {
	ch := make(chan *block.Block)

	sub, err := x.WSClient.ReceiveBlocks(nil, ch)
	if err != nil {
		return nil, fmt.Errorf("listen new blocks over Neo RPC WebSocket: %w", err)
	}

	x.subsMtx.Lock()
	x.subs = append(x.subs, sub)
	x.subsMtx.Unlock()

	return ch, nil
}

// SubscribeToNotaryRequests implements [deploy.Blockchain] interface.
func (x *neoFSSidechain) SubscribeToNotaryRequests() (<-chan *result.NotaryRequestEvent, error) {
	ch := make(chan *result.NotaryRequestEvent)

	sub, err := x.WSClient.ReceiveNotaryRequests(nil, ch)
	if err != nil {
		return nil, fmt.Errorf("listen notary requests over Neo RPC WebSocket: %w", err)
	}

	x.subsMtx.Lock()
	x.subs = append(x.subs, sub)
	x.subsMtx.Unlock()

	return ch, nil
}

func newNeoFSSidechain(sidechainClient *client.Client, sidechainWSClient *rpcclient.WSClient) *neoFSSidechain {
	return &neoFSSidechain{
		client:   sidechainClient,
		WSClient: sidechainWSClient,
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
