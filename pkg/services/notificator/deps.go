package notificator

import objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"

// NotificationSource is a source of object notifications.
type NotificationSource interface {
	// Iterate must iterate over all notifications for the
	// provided epoch and call handler for all of them.
	Iterate(epoch uint64, handler func(topic string, addr *objectSDKAddress.Address))
}

// NotificationWriter notifies all the subscribers
// about new object notifications.
type NotificationWriter interface {
	// Notify must send string representation of the object
	// address into the specified topic.
	Notify(topic string, address *objectSDKAddress.Address)
}
