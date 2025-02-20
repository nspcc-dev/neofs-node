package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// AnnounceLoadNotaryEvent is method name for announce load estimation operation
	// in `Container` contract. Is used as identificator for notary
	// announce load estimation requests.
	AnnounceLoadNotaryEvent = "putContainerSize"
)

// ParseAnnounceLoadNotary from NotaryEvent into container event structure.
func ParseAnnounceLoadNotary(ne event.NotaryEvent) (event.Event, error) {
	const expectedItemNumAnnounceLoad = 4
	var ev AnnounceLoad

	fieldNum := 0

	for _, op := range ne.Params() {
		switch fieldNum {
		case 3:
			n, err := event.IntFromOpcode(op)
			if err != nil {
				return nil, err
			}

			ev.epoch = uint64(n)
		case 2:
			data, err := event.BytesFromOpcode(op)
			if err != nil {
				return nil, err
			}

			ev.cnrID = data
		case 1:
			n, err := event.IntFromOpcode(op)
			if err != nil {
				return nil, err
			}

			ev.val = uint64(n)
		case 0:
			data, err := event.BytesFromOpcode(op)
			if err != nil {
				return nil, err
			}

			ev.key = data
		case expectedItemNumAnnounceLoad:
			return nil, event.UnexpectedArgNumErr(AnnounceLoadNotaryEvent)
		}
		fieldNum++
	}

	ev.notaryRequest = ne.Raw()

	return ev, nil
}
