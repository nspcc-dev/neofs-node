package notificator

import (
	"fmt"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Prm groups Notificator constructor's
// parameters. All are required.
type Prm struct {
	writer             NotificationWriter
	notificationSource NotificationSource
	logger             *zap.Logger
}

// SetLogger sets a logger.
func (prm *Prm) SetLogger(v *zap.Logger) *Prm {
	prm.logger = v
	return prm
}

// SetWriter sets notification writer.
func (prm *Prm) SetWriter(v NotificationWriter) *Prm {
	prm.writer = v
	return prm
}

// SetNotificationSource sets notification source.
func (prm *Prm) SetNotificationSource(v NotificationSource) *Prm {
	prm.notificationSource = v
	return prm
}

// Notificator is a notification producer that handles
// objects with defined notification epoch.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Notificator struct {
	w  NotificationWriter
	ns NotificationSource
	l  *zap.Logger
}

// New creates, initializes and returns the Notificator instance.
//
// Panics if any field of the passed Prm structure is not set/set
// to nil.
func New(prm *Prm) *Notificator {
	panicOnNil := func(v interface{}, name string) {
		if v == nil {
			panic(fmt.Sprintf("Notificator constructor: %s is nil\n", name))
		}
	}

	panicOnNil(prm.writer, "NotificationWriter")
	panicOnNil(prm.notificationSource, "NotificationSource")
	panicOnNil(prm.logger, "Logger")

	return &Notificator{
		w:  prm.writer,
		ns: prm.notificationSource,
		l:  prm.logger,
	}
}

// ProcessEpoch looks for all objects with defined epoch in the storage
// and passes their addresses to the NotificationWriter.
func (n *Notificator) ProcessEpoch(epoch uint64) {
	logger := n.l.With(zap.Uint64("epoch", epoch))
	logger.Debug("notificator: start processing object notifications")

	n.ns.Iterate(epoch, func(topic string, addr oid.Address) {
		n.l.Debug("notificator: processing object notification",
			zap.String("topic", topic),
			zap.Stringer("address", addr),
		)

		n.w.Notify(topic, addr)
	})
}
