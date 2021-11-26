package subnetevents

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// RemoveNode structure of subnet.RemoveNode notification from morph chain.
type RemoveNode struct {
	subnetID []byte
	nodeKey  []byte

	// txHash is used in notary environmental
	// for calculating unique but same for
	// all notification receivers values.
	txHash util.Uint256
}

// MorphEvent implements Neo:Morph Event interface.
func (RemoveNode) MorphEvent() {}

// SubnetworkID returns a marshalled subnetID structure, defined in API.
func (rn RemoveNode) SubnetworkID() []byte { return rn.subnetID }

// Node is public key of the nodeKey that is being deleted.
func (rn RemoveNode) Node() []byte { return rn.nodeKey }

// TxHash returns hash of the TX with RemoveNode
// notification.
func (rn RemoveNode) TxHash() util.Uint256 { return rn.txHash }

const expectedItemNumRemoveNode = 2

// ParseRemoveNode parses notification into subnet event structure.
//
// Expects 2 stack items.
func ParseRemoveNode(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  RemoveNode
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != expectedItemNumRemoveNode {
		return nil, event.WrongNumberOfParameters(expectedItemNumRemoveNode, ln)
	}

	ev.subnetID, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get raw subnetID: %w", err)
	}

	ev.nodeKey, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get raw public key of the node: %w", err)
	}

	ev.txHash = e.Container

	return ev, nil
}
