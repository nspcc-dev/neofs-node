package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// PutReportNotaryEvent is method name for announce load report operation
	// in `Container` contract. Is used as an identificator for notary
	// announce load report requests.
	PutReportNotaryEvent = "putEstimation"
)

// Report structure of container.PutEstimation notification from morph chain.
type Report struct {
	CID           []byte
	StorageSize   int64
	ObjectsNumber int64
	NodeKey       []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	NotaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements [event.Event].
func (r Report) MorphEvent() {}

// ParsePutReport from NotaryEvent into container event structure.
func ParsePutReport(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumAnnounceLoad = 4
	var ev Report
	var err error

	opcodes := ne.Params()
	if len(opcodes) != expectedItemNumAnnounceLoad {
		return nil, event.UnexpectedArgNumErr(PutReportNotaryEvent)
	}
	ev.NodeKey, err = event.BytesFromOpcode(opcodes[0])
	if err != nil {
		return nil, fmt.Errorf("reporter's public key parsing: %w", err)
	}
	ev.ObjectsNumber, err = event.IntFromOpcode(opcodes[1])
	if err != nil {
		return nil, fmt.Errorf("container's objects number parsing: %w", err)
	}
	ev.StorageSize, err = event.IntFromOpcode(opcodes[2])
	if err != nil {
		return nil, fmt.Errorf("container's size parsing: %w", err)
	}
	ev.CID, err = event.BytesFromOpcode(opcodes[3])
	if err != nil {
		return nil, fmt.Errorf("container ID parsing: %w", err)
	}

	ev.NotaryRequest = ne.Raw()

	return ev, nil
}
