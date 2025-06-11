package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

func getItemsFromNotification(notifEvent *state.ContainedNotificationEvent, expectedNum int) ([]stackitem.Item, error) {
	items := notifEvent.Item.Value().([]stackitem.Item)
	if len(items) != expectedNum {
		return nil, fmt.Errorf("wrong/unsupported item num %d instead of %d", len(items), expectedNum)
	}
	return items, nil
}

func restoreItemValue[T any](items []stackitem.Item, i int, desc string, typ stackitem.Type, f func(stackitem.Item) (T, error)) (v T, err error) {
	v, err = f(items[i])
	if err != nil {
		return v, fmt.Errorf("item#%d (%s, %s): %w", i, typ, desc, err)
	}
	return v, nil
}

// Created is a container creation event thrown by Container contract.
type Created struct {
	event.Event
	ID    cid.ID
	Owner user.ID
}

// RestoreCreated restores [Created] event from the notification one.
func RestoreCreated(notifEvent *state.ContainedNotificationEvent) (event.Event, error) {
	items, err := getItemsFromNotification(notifEvent, 2)
	if err != nil {
		return nil, err
	}

	id, err := restoreItemValue(items, 0, "ID", stackitem.ByteArrayT, client.BytesFromStackItem)
	if err != nil {
		return nil, err
	}
	owner, err := restoreItemValue(items, 1, "owner", stackitem.ByteArrayT, client.BytesFromStackItem)
	if err != nil {
		return nil, err
	}

	var res Created

	if err = res.ID.Decode(id); err != nil {
		return nil, fmt.Errorf("decode ID item: %w", err)
	}
	// TODO: replace message decoding after https://github.com/nspcc-dev/neofs-sdk-go/issues/669
	if err = res.Owner.FromProtoMessage(&refs.OwnerID{Value: owner}); err != nil {
		return nil, fmt.Errorf("decode owner item: %w", err)
	}

	return res, nil
}

// Removed is a container removal event thrown by Container contract.
type Removed struct {
	event.Event
	ID    cid.ID
	Owner user.ID
}

// RestoreRemoved restores [Removed] event from the notification one.
func RestoreRemoved(notifEvent *state.ContainedNotificationEvent) (event.Event, error) {
	items, err := getItemsFromNotification(notifEvent, 2)
	if err != nil {
		return nil, err
	}

	id, err := restoreItemValue(items, 0, "ID", stackitem.ByteArrayT, client.BytesFromStackItem)
	if err != nil {
		return nil, err
	}
	owner, err := restoreItemValue(items, 1, "owner", stackitem.ByteArrayT, client.BytesFromStackItem)
	if err != nil {
		return nil, err
	}

	var res Removed

	if err = res.ID.Decode(id); err != nil {
		return nil, fmt.Errorf("decode ID item: %w", err)
	}
	// TODO: replace message decoding after https://github.com/nspcc-dev/neofs-sdk-go/issues/669
	if err = res.Owner.FromProtoMessage(&refs.OwnerID{Value: owner}); err != nil {
		return nil, fmt.Errorf("decode owner item: %w", err)
	}

	return res, nil
}

// EACLChanged is a container eACL change event thrown by Container contract.
type EACLChanged struct {
	event.Event
	Container cid.ID
}

// RestoreEACLChanged restores [EACLChanged] event from the notification one.
func RestoreEACLChanged(notifEvent *state.ContainedNotificationEvent) (event.Event, error) {
	items, err := getItemsFromNotification(notifEvent, 1)
	if err != nil {
		return nil, err
	}

	id, err := restoreItemValue(items, 0, "ID", stackitem.ByteArrayT, client.BytesFromStackItem)
	if err != nil {
		return nil, err
	}

	var res EACLChanged

	if err = res.Container.Decode(id); err != nil {
		return nil, fmt.Errorf("decode container ID item: %w", err)
	}

	return res, nil
}
