package container

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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
	args, err := event.GetArgs(ne, expectedItemNumAnnounceLoad)
	if err != nil {
		return nil, err
	}
	var ev Report

	ev.CID, err = event.GetValueFromArg(args, 0, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.StorageSize, err = event.GetValueFromArg(args, 1, ne.Type().String(), scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	ev.ObjectsNumber, err = event.GetValueFromArg(args, 2, ne.Type().String(), scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	ev.NodeKey, err = event.GetValueFromArg(args, 3, ne.Type().String(), scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}

	ev.NotaryRequest = ne.Raw()

	return ev, nil
}
