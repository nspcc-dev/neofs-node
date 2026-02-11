package container

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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
	args, err := getArgsFromEvent(ne, expectedItemNumAnnounceLoad)
	if err != nil {
		return nil, err
	}
	var ev Report

	ev.CID, err = getValueFromArg(args, 0, "container ID", stackitem.ByteArrayT, scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}
	ev.StorageSize, err = getValueFromArg(args, 1, "container's size", stackitem.IntegerT, scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	ev.ObjectsNumber, err = getValueFromArg(args, 2, "objects number", stackitem.IntegerT, scparser.GetInt64FromInstr)
	if err != nil {
		return nil, err
	}
	ev.NodeKey, err = getValueFromArg(args, 3, "reporter's key", stackitem.ByteArrayT, scparser.GetBytesFromInstr)
	if err != nil {
		return nil, err
	}

	ev.NotaryRequest = ne.Raw()

	return ev, nil
}
