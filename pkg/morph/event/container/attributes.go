package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

const (
	// AttributeChagedEvent is notification that is produced after any
	// attribute in any container is changed in FS chain.
	AttributeChagedEvent = "AttributeChanged"
)

// AttributeChanged is notification on container attribute changes.
type AttributeChanged struct {
	cID cid.ID
	key string
}

// Container returns updated container's ID.
func (a AttributeChanged) Container() cid.ID {
	return a.cID
}

// Attribute returns updated attribute's key.
func (a AttributeChanged) Attribute() string {
	return a.key
}

func (a AttributeChanged) MorphEvent() {}

// ParseAttributeChangedEvent from notification into [AttributeChanged] structure.
func ParseAttributeChangedEvent(e *state.ContainedNotificationEvent) (event.Event, error) {
	var rpcEv containerrpc.AttributeChangedEvent
	err := rpcEv.FromStackItem(e.Item)
	if err != nil {
		return nil, fmt.Errorf("could not parse notify event from stack item: %w", err)
	}

	cID, err := cid.DecodeBytes(rpcEv.ContainerID[:])
	if err != nil {
		return nil, fmt.Errorf("could not decode container ID: %w", err)
	}

	return AttributeChanged{
		cID: cID,
		key: rpcEv.Attribute,
	}, nil
}
