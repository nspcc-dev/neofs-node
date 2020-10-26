package container

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Delete structure of container.Delete notification from morph chain.
type Delete struct {
	containerID []byte
	signature   []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (Delete) MorphEvent() {}

// Container is a marshalled container structure, defined in API.
func (d Delete) ContainerID() []byte { return d.containerID }

// Signature of marshalled container by container owner.
func (d Delete) Signature() []byte { return d.signature }

// ParseDelete from notification into container event structure.
func ParseDelete(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Delete
		err error
	)

	if ln := len(params); ln != 2 {
		return nil, event.WrongNumberOfParameters(2, ln)
	}

	// parse container
	ev.containerID, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get container")
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get signature")
	}

	return ev, nil
}
