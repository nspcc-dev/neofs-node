package main

import (
	"fmt"

	"github.com/mr-tron/base58"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/notificator"
	"github.com/nspcc-dev/neofs-node/pkg/services/notificator/nats"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type notificationSource struct {
	e            *engine.StorageEngine
	l            *zap.Logger
	defaultTopic string
}

func (n *notificationSource) Iterate(epoch uint64, handler func(topic string, addr oid.Address)) {
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
	a oid.Address,
	h func(topic string, addr oid.Address),
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

type notificationWriter struct {
	l *zap.Logger
	w *nats.Writer
}

func (n notificationWriter) Notify(topic string, address oid.Address) {
	if err := n.w.Notify(topic, address); err != nil {
		n.l.Warn("could not write object notification",
			zap.Stringer("address", address),
			zap.String("topic", topic),
			zap.Error(err),
		)
	}
}

func initNotifications(c *cfg) {
	if nodeconfig.Notification(c.appCfg).Enabled() {
		topic := nodeconfig.Notification(c.appCfg).DefaultTopic()
		pubKey := base58.Encode(c.cfgNodeInfo.localInfo.PublicKey())

		if topic == "" {
			topic = pubKey
		}

		natsSvc := nats.New(
			nats.WithConnectionName("NeoFS Storage Node: "+pubKey), // connection name is used in the server side logs
			nats.WithTimeout(nodeconfig.Notification(c.appCfg).Timeout()),
			nats.WithClientCert(
				nodeconfig.Notification(c.appCfg).CertPath(),
				nodeconfig.Notification(c.appCfg).KeyPath(),
			),
			nats.WithRootCA(nodeconfig.Notification(c.appCfg).CAPath()),
			nats.WithLogger(c.log),
		)

		c.cfgNotifications = cfgNotifications{
			enabled: true,
			nw: notificationWriter{
				l: c.log,
				w: natsSvc,
			},
			defaultTopic: topic,
		}

		n := notificator.New(new(notificator.Prm).
			SetLogger(c.log).
			SetNotificationSource(
				&notificationSource{
					e:            c.cfgObject.cfgLocalStorage.localStorage,
					l:            c.log,
					defaultTopic: topic,
				}).
			SetWriter(c.cfgNotifications.nw),
		)

		addNewEpochAsyncNotificationHandler(c, func(e event.Event) {
			ev := e.(netmap.NewEpoch)

			n.ProcessEpoch(ev.EpochNumber())
		})
	}
}

func connectNats(c *cfg) {
	if !c.cfgNotifications.enabled {
		return
	}

	endpoint := nodeconfig.Notification(c.appCfg).Endpoint()
	err := c.cfgNotifications.nw.w.Connect(c.ctx, endpoint)
	if err != nil {
		panic(fmt.Sprintf("could not connect to a nats endpoint %s: %v", endpoint, err))
	}
}
