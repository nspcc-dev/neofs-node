package main

import (
	"fmt"

	"github.com/mr-tron/base58"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/notificator"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

type simpleNotificationWriter struct {
	l *zap.Logger
}

func (s simpleNotificationWriter) Notify(topic string, address *addressSDK.Address) {
	s.l.Debug("got notification to write",
		zap.String("topic", topic),
		zap.Stringer("address", address),
	)
}

type notificationSource struct {
	e            *engine.StorageEngine
	l            *zap.Logger
	defaultTopic string
}

func (n *notificationSource) Iterate(epoch uint64, handler func(topic string, addr *addressSDK.Address)) {
	log := n.l.With(zap.Uint64("epoch", epoch))

	listRes, err := n.e.ListContainers(engine.ListContainersPrm{})
	if err != nil {
		log.Error("notificator: could not list containers", zap.Error(err))
		return
	}

	filters := objectSDK.NewSearchFilters()
	filters.AddNotificationEpochFilter(epoch)

	selectPrm := new(engine.SelectPrm)
	selectPrm.WithFilters(filters)

	for _, c := range listRes.Containers() {
		selectPrm.WithContainerID(c)

		selectRes, err := n.e.Select(selectPrm)
		if err != nil {
			log.Error("notificator: could not select objects from container",
				zap.Stringer("cid", c),
				zap.Error(err),
			)
			continue
		}

		for _, a := range selectRes.AddressList() {
			err = n.processAddress(a, handler)
			if err != nil {
				log.Error("notificator: could not process object",
					zap.Stringer("address", a),
					zap.Error(err),
				)
				continue
			}
		}
	}

	log.Debug("notificator: finished processing object notifications")
}

func (n *notificationSource) processAddress(
	a *addressSDK.Address,
	h func(topic string, addr *addressSDK.Address),
) error {
	prm := new(engine.HeadPrm)
	prm.WithAddress(a)

	res, err := n.e.Head(prm)
	if err != nil {
		return err
	}

	ni, err := res.Header().NotificationInfo()
	if err != nil {
		return fmt.Errorf("could not retreive notification topic from object: %w", err)
	}

	topic := ni.Topic()

	if topic == "" {
		topic = n.defaultTopic
	}

	h(topic, a)

	return nil
}

func initNotification(c *cfg) {
	if nodeconfig.Notification(c.appCfg).Enabled() {
		topic := nodeconfig.Notification(c.appCfg).DefaultTopic()

		if topic == "" {
			topic = base58.Encode(c.cfgNodeInfo.localInfo.PublicKey())
		}

		n := notificator.New(new(notificator.Prm).
			SetLogger(c.log).
			SetNotificationSource(
				&notificationSource{
					e:            c.cfgObject.cfgLocalStorage.localStorage,
					l:            c.log,
					defaultTopic: topic,
				}).
			SetWriter(simpleNotificationWriter{l: c.log}),
		)

		addNewEpochAsyncNotificationHandler(c, func(e event.Event) {
			ev := e.(netmap.NewEpoch)

			n.ProcessEpoch(ev.EpochNumber())
		})
	}
}
