package innerring

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// [deploy.Blockchain] methods provided from both [client.Client] and
// [rpcclient.WSClient].
type fsChainCommonRPC interface {
	notary.RPCActor
	GetCommittee() (keys.PublicKeys, error)
	GetContractStateByID(id int32) (*state.Contract, error)
	GetContractStateByHash(util.Uint160) (*state.Contract, error)
}

type fsChain struct {
	fsChainCommonRPC

	client *client.Client

	wsClient *rpcclient.WSClient

	subsMtx sync.Mutex
	subs    []string
}

// cancels all active subscriptions. Must not be called concurrently with
// subscribe methods.
func (x *fsChain) cancelSubs() {
	if x.wsClient != nil {
		for i := range x.subs {
			_ = x.wsClient.Unsubscribe(x.subs[i])
		}
		return
	}
}

// SubscribeToNewHeaders implements [deploy.Blockchain] interface.
func (x *fsChain) SubscribeToNewHeaders() (<-chan *block.Header, error) {
	if x.wsClient != nil {
		ch := make(chan *block.Header)

		sub, err := x.wsClient.ReceiveHeadersOfAddedBlocks(nil, ch)
		if err != nil {
			return nil, fmt.Errorf("listen to new blocks over Neo RPC WebSocket: %w", err)
		}

		x.subsMtx.Lock()
		x.subs = append(x.subs, sub)
		x.subsMtx.Unlock()

		return ch, nil
	}

	err := x.client.ReceiveHeaders()
	if err != nil {
		return nil, fmt.Errorf("listen to new blocks over Neo RPC WebSocket multi-endpoint: %w", err)
	}

	_, headerCh, _ := x.client.Notifications()

	return headerCh, nil
}

// SubscribeToNotaryRequests implements [deploy.Blockchain] interface.
func (x *fsChain) SubscribeToNotaryRequests() (<-chan *result.NotaryRequestEvent, error) {
	if x.wsClient != nil {
		var (
			ch     = make(chan *result.NotaryRequestEvent)
			evtype = mempoolevent.TransactionAdded
		)

		sub, err := x.wsClient.ReceiveNotaryRequests(&neorpc.NotaryRequestFilter{Type: &evtype}, ch)
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
func newFSChain(fsChainClient *client.Client, fsChainWSClient *rpcclient.WSClient) *fsChain {
	res := &fsChain{
		client:   fsChainClient,
		wsClient: fsChainWSClient,
	}

	if fsChainWSClient != nil {
		res.fsChainCommonRPC = fsChainWSClient
	} else {
		res.fsChainCommonRPC = fsChainClient
	}

	return res
}
