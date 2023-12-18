package innerring

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// [deploy.Blockchain] methods provided from both [client.Client] and
// [rpcclient.WSClient].
type neoFSSidechainCommonRPC interface {
	notary.RPCActor
	GetCommittee() (keys.PublicKeys, error)
	GetContractStateByID(id int32) (*state.Contract, error)
	GetContractStateByHash(util.Uint160) (*state.Contract, error)
}

type neoFSSidechain struct {
	neoFSSidechainCommonRPC

	client *client.Client

	wsClient *rpcclient.WSClient

	subsMtx sync.Mutex
	subs    []string
}

// cancels all active subscriptions. Must not be called concurrently with
// subscribe methods.
func (x *neoFSSidechain) cancelSubs() {
	if x.wsClient != nil {
		for i := range x.subs {
			_ = x.wsClient.Unsubscribe(x.subs[i])
		}
		return
	}
}

// SubscribeToNewBlocks implements [deploy.Blockchain] interface.
func (x *neoFSSidechain) SubscribeToNewBlocks() (<-chan *block.Block, error) {
	if x.wsClient != nil {
		ch := make(chan *block.Block)

		sub, err := x.wsClient.ReceiveBlocks(nil, ch)
		if err != nil {
			return nil, fmt.Errorf("listen to new blocks over Neo RPC WebSocket: %w", err)
		}

		x.subsMtx.Lock()
		x.subs = append(x.subs, sub)
		x.subsMtx.Unlock()

		return ch, nil
	}

	err := x.client.ReceiveBlocks()
	if err != nil {
		return nil, fmt.Errorf("listen to new blocks over Neo RPC WebSocket multi-endpoint: %w", err)
	}

	_, ch, _ := x.client.Notifications()
	return ch, nil
}

// SubscribeToNotaryRequests implements [deploy.Blockchain] interface.
func (x *neoFSSidechain) SubscribeToNotaryRequests() (<-chan *result.NotaryRequestEvent, error) {
	if x.wsClient != nil {
		ch := make(chan *result.NotaryRequestEvent)

		sub, err := x.wsClient.ReceiveNotaryRequests(nil, ch)
		if err != nil {
			return nil, fmt.Errorf("listen to notary requests over Neo RPC WebSocket: %w", err)
		}

		x.subsMtx.Lock()
		x.subs = append(x.subs, sub)
		x.subsMtx.Unlock()

		return ch, nil
	}

	err := x.client.ReceiveAllNotaryRequests()
	if err != nil {
		return nil, fmt.Errorf("listen to notary requests over Neo RPC WebSocket multi-endpoint: %w", err)
	}

	_, _, ch := x.client.Notifications()
	return ch, nil
}

// second parameter is optional and affects subscription methods.
func newNeoFSSidechain(sidechainClient *client.Client, sidechainWSClient *rpcclient.WSClient) *neoFSSidechain {
	res := &neoFSSidechain{
		client:   sidechainClient,
		wsClient: sidechainWSClient,
	}

	if sidechainWSClient != nil {
		res.neoFSSidechainCommonRPC = sidechainWSClient
	} else {
		res.neoFSSidechainCommonRPC = sidechainClient
	}

	return res
}
