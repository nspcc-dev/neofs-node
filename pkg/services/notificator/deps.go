package notificator

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// NotificationSource is a source of object notifications.
type NotificationSource interface {
	// Iterate must iterate over all notifications for the
	// provided epoch and call handler for all of them.
	Iterate(epoch uint64, handler func(topic string, addr oid.Address))
}

// NotificationWriter notifies all the subscribers
// about new object notifications.
type NotificationWriter interface {
	// Notify must notify about an event generated
	// from an object with a specific topic.
	Notify(topic string, address oid.Address)
}
