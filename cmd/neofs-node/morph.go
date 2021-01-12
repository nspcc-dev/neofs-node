package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/zap"
)

const newEpochNotification = "NewEpoch"

var (
	errNoRPCEndpoints = errors.New("NEO RPC endpoints are not specified in config")
	errNoWSEndpoints  = errors.New("websocket NEO listener endpoints are not specified in config")
)

func initMorphComponents(c *cfg) {
	var err error

	addresses := c.viper.GetStringSlice(cfgMorphRPCAddress)
	if len(addresses) == 0 {
		fatalOnErr(errNoRPCEndpoints)
	}

	crand := rand.New() // math/rand with cryptographic source
	crand.Shuffle(len(addresses), func(i, j int) {
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})

	for i := range addresses {
		c.cfgMorph.client, err = client.New(c.key, addresses[i])
		if err == nil {
			c.log.Info("neo RPC connection established",
				zap.String("endpoint", addresses[i]))

			break
		}

		c.log.Info("failed to establish neo RPC connection, trying another",
			zap.String("endpoint", addresses[i]),
			zap.String("error", err.Error()))
	}

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
	var (
		err  error
		subs subscriber.Subscriber
	)

	endpoints := c.viper.GetStringSlice(cfgMorphNotifyRPCAddress)
	if len(endpoints) == 0 {
		fatalOnErr(errNoWSEndpoints)
	}

	timeout := c.viper.GetDuration(cfgMorphNotifyDialTimeout)

	crand := rand.New() // math/rand with cryptographic source
	crand.Shuffle(len(endpoints), func(i, j int) {
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	})

	for i := range endpoints {
		subs, err = subscriber.New(c.ctx, &subscriber.Params{
			Log:         c.log,
			Endpoint:    endpoints[i],
			DialTimeout: timeout,
		})
		if err == nil {
			c.log.Info("websocket neo event listener established",
				zap.String("endpoint", endpoints[i]))

			break
		}

		c.log.Info("failed to establish websocket neo event listener, trying another",
			zap.String("endpoint", endpoints[i]),
			zap.String("error", err.Error()))
	}

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
