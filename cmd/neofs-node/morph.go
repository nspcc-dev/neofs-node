package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/util"
	mainchainconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/mainchain"
	morphconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/morph"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/zap"
)

const newEpochNotification = "NewEpoch"

func initMorphComponents(c *cfg) {
	var err error

	fn := func(addresses []string, dialTimeout time.Duration, handler func(*client.Client)) {
		crand := rand.New() // math/rand with cryptographic source
		crand.Shuffle(len(addresses), func(i, j int) {
			addresses[i], addresses[j] = addresses[j], addresses[i]
		})

		cli, err := client.New(c.key, addresses[0],
			client.WithDialTimeout(dialTimeout),
			client.WithLogger(c.log),
			client.WithExtraEndpoints(addresses[1:]),
		)
		if err == nil {
			handler(cli)

			return
		}

		c.log.Info("failed to create neo RPC client",
			zap.Any("endpoints", addresses),
			zap.String("error", err.Error()),
		)

		fatalOnErr(err)
	}

	// replace to a separate initialing block during refactoring
	// since current function initializes sidechain components
	fn(mainchainconfig.RPCEndpoint(c.appCfg), mainchainconfig.DialTimeout(c.appCfg), func(cli *client.Client) {
		c.mainChainClient = cli

		c.log.Debug("notary support",
			zap.Bool("mainchain_enabled", cli.ProbeNotary()),
		)
	})

	fn(morphconfig.RPCEndpoint(c.appCfg), morphconfig.DialTimeout(c.appCfg), func(cli *client.Client) {
		c.cfgMorph.client = cli

		c.log.Debug("notary support",
			zap.Bool("sidechain_enabled", cli.ProbeNotary()),
		)
	})

	wrap, err := wrapper.NewFromMorph(c.cfgMorph.client, c.cfgNetmap.scriptHash, 0)
	fatalOnErr(err)

	var netmapSource netmap.Source

	c.cfgMorph.disableCache = morphconfig.DisableCache(c.appCfg)

	if c.cfgMorph.disableCache {
		netmapSource = wrap
	} else {
		// use RPC node as source of netmap (with caching)
		netmapSource = newCachedNetmapStorage(c.cfgNetmap.state, wrap)
	}

	c.cfgObject.netMapSource = netmapSource
	c.cfgNetmap.wrapper = wrap
}

func listenMorphNotifications(c *cfg) {
	var (
		err  error
		subs subscriber.Subscriber
	)

	endpoints := morphconfig.NotificationEndpoint(c.appCfg)
	timeout := morphconfig.DialTimeout(c.appCfg)

	crand := rand.New() // math/rand with cryptographic source
	crand.Shuffle(len(endpoints), func(i, j int) {
		endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	})

	fromSideChainBlock, err := c.persistate.UInt32(persistateSideChainLastBlockKey)
	if err != nil {
		fromSideChainBlock = 0
		c.log.Warn("can't get last processed side chain block number", zap.String("error", err.Error()))
	}

	for i := range endpoints {
		subs, err = subscriber.New(c.ctx, &subscriber.Params{
			Log:            c.log,
			Endpoint:       endpoints[i],
			DialTimeout:    timeout,
			StartFromBlock: fromSideChainBlock,
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
	registerNotificationHandlers(c.cfgContainer.scriptHash, lis, c.cfgContainer.parsers, c.cfgContainer.subscribers)

	registerBlockHandler(lis, func(block *block.Block) {
		c.log.Debug("new block", zap.Uint32("index", block.Index))

		err = c.persistate.SetUInt32(persistateSideChainLastBlockKey, block.Index)
		if err != nil {
			c.log.Warn("can't update persistent state",
				zap.String("chain", "side"),
				zap.Uint32("block_index", block.Index))
		}

		tickBlockTimers(c)
	})
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

func registerBlockHandler(lis event.Listener, handler event.BlockHandler) {
	lis.RegisterBlockHandler(handler)
}
