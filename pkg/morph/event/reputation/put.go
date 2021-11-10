package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

// Put structure of reputation.reputationPut notification from
// morph chain.
type Put struct {
	epoch  uint64
	peerID reputation.PeerID
	value  reputation.GlobalTrust

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

const peerIDLength = 33 // compressed public key

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Epoch returns epoch value of reputation data.
func (p Put) Epoch() uint64 {
	return p.epoch
}

// PeerID returns peer id of reputation data.
func (p Put) PeerID() reputation.PeerID {
	return p.peerID
}

// Value returns reputation structure.
func (p Put) Value() reputation.GlobalTrust {
	return p.value
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (p Put) NotaryRequest() *payload.P2PNotaryRequest {
	return p.notaryRequest
}

// ParsePut from notification into reputation event structure.
func ParsePut(prms []stackitem.Item) (event.Event, error) {
	var (
		ev  Put
		err error
	)

	if ln := len(prms); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse epoch number
	epoch, err := client.IntFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get integer epoch number: %w", err)
	}

	ev.epoch = uint64(epoch)

	// parse peer ID value
	peerID, err := client.BytesFromStackItem(prms[1])
	if err != nil {
		return nil, fmt.Errorf("could not get peer ID value: %w", err)
	}

	if ln := len(peerID); ln != peerIDLength {
		return nil, fmt.Errorf("peer ID is %d byte long, expected %d", ln, peerIDLength)
	}

	var publicKey [33]byte
	copy(publicKey[:], peerID)
	ev.peerID.SetPublicKey(publicKey)

	// parse global trust value
	rawValue, err := client.BytesFromStackItem(prms[2])
	if err != nil {
		return nil, fmt.Errorf("could not get global trust value: %w", err)
	}

	err = ev.value.Unmarshal(rawValue)
	if err != nil {
		return nil, fmt.Errorf("could not parse global trust value: %w", err)
	}

	return ev, nil
}
