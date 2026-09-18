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

	// ContainerUpdatedEvent is notification that is produced after any
	// changes have been applied to a container and its revision has
	// incremented.
	ContainerUpdatedEvent = "ContainerUpdated"
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

// ContainerUpdated is notification on container revision increment.
type ContainerUpdated struct {
	cID      cid.ID
	revision uint64
}

// Container returns updated container's ID.
func (c ContainerUpdated) Container() cid.ID {
	return c.cID
}

// Revision returns new revision.
func (c ContainerUpdated) Revision() uint64 {
	return c.revision
}

func (c ContainerUpdated) MorphEvent() {}

// ParseContainerUpdatedEvent from notification into [ContainerUpdated] structure.
func ParseContainerUpdatedEvent(e *state.ContainedNotificationEvent) (event.Event, error) {
	var rpcEv containerrpc.ContainerUpdatedEvent
	err := rpcEv.FromStackItem(e.Item)
	if err != nil {
		return nil, fmt.Errorf("could not parse notify event from stack item: %w", err)
	}

	cID, err := cid.DecodeBytes(rpcEv.Container[:])
	if err != nil {
		return nil, fmt.Errorf("could not decode container ID: %w", err)
	}

	if !rpcEv.Revision.IsInt64() {
		return nil, fmt.Errorf("revision is not an integer number")
	}
	revision := rpcEv.Revision.Int64()
	if revision <= 0 {
		return nil, fmt.Errorf("non-positive container revision: %d", revision)
	}

	return ContainerUpdated{
		cID:      cID,
		revision: uint64(revision),
	}, nil
}
