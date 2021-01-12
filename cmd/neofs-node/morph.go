package main

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
)

const newEpochNotification = "NewEpoch"

func initMorphComponents(c *cfg) {
	var err error

	c.cfgMorph.client, err = client.New(c.key, c.viper.GetString(cfgMorphRPCAddress))
	fatalOnErr(err)

	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgNetmap.scriptHash,
		c.cfgContainer.fee,
	)
	fatalOnErr(err)

	cli, err := netmap.New(staticClient)
	fatalOnErr(err)

	wrap, err := wrapper.New(cli)
	fatalOnErr(err)

	c.cfgObject.netMapStorage = wrap
	c.cfgNetmap.wrapper = wrap
}

func listenMorphNotifications(c *cfg) {
	subs, err := subscriber.New(c.ctx, &subscriber.Params{
		Log:         c.log,
		Endpoint:    c.viper.GetString(cfgMorphNotifyRPCAddress),
		DialTimeout: c.viper.GetDuration(cfgMorphNotifyDialTimeout),
	})
	fatalOnErr(err)

	lis, err := event.NewListener(event.ListenerParams{
		Logger:     c.log,
		Subscriber: subs,
	})
	fatalOnErr(err)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		lis.ListenWithError(ctx, c.internalErr)
	}))

	setNetmapNotificationParser(c, newEpochNotification, netmapEvent.ParseNewEpoch)
	registerNotificationHandlers(c.cfgNetmap.scriptHash, lis, c.cfgNetmap.parsers, c.cfgNetmap.subscribers)
}

func registerNotificationHandlers(scHash util.Uint160, lis event.Listener, parsers map[event.Type]event.Parser,
	subs map[event.Type][]event.Handler) {
	for typ, handlers := range subs {
		pi := event.ParserInfo{}
		pi.SetType(typ)
		pi.SetScriptHash(scHash)

		p, ok := parsers[typ]
		if !ok {
			panic(fmt.Sprintf("missing parser for event %s", typ))
		}

		pi.SetParser(p)

		lis.SetParser(pi)

		for _, h := range handlers {
			hi := event.HandlerInfo{}
			hi.SetType(typ)
			hi.SetScriptHash(scHash)
			hi.SetHandler(h)

			lis.RegisterHandler(hi)
		}
	}
}
