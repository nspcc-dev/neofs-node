package main

import (
	"context"
	"errors"
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

const (
	newEpochNotification = "NewEpoch"

	// notaryDepositExtraBlocks is amount of extra blocks to overlap two deposits,
	// we do that to make sure that there won't be any blocks without deposited
	// assets in notary contract; make sure it is bigger than any extra rounding
	// value in notary client.
	notaryDepositExtraBlocks = 300

	// amount of tries(blocks) before notary deposit timeout.
	notaryDepositRetriesAmount
)

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

		c.cfgMorph.notaryEnabled = cli.ProbeNotary()

		lookupScriptHashesInNNS(c) // smart contract auto negotiation

		if c.cfgMorph.notaryEnabled {
			err = c.cfgMorph.client.EnableNotarySupport(
				client.WithProxyContract(
					c.cfgMorph.proxyScriptHash,
				),
			)
			fatalOnErr(err)

			c.cfgMorph.notaryDepositAmount = morphconfig.Notary(c.appCfg).Amount()
			c.cfgMorph.notaryDepositDuration = morphconfig.Notary(c.appCfg).Duration()

			newDepositTimer(c)
		}

		c.log.Debug("notary support",
			zap.Bool("sidechain_enabled", c.cfgMorph.notaryEnabled),
		)
	})

	wrap, err := wrapper.NewFromMorph(c.cfgMorph.client, c.cfgNetmap.scriptHash, 0, wrapper.TryNotary())
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

func makeAndWaitNotaryDeposit(c *cfg) {
	// skip notary deposit in non-notary environments
	if !c.cfgMorph.notaryEnabled {
		return
	}

	tx, err := makeNotaryDeposit(c)
	fatalOnErr(err)

	err = waitNotaryDeposit(c, tx)
	fatalOnErr(err)
}

func makeNotaryDeposit(c *cfg) (util.Uint256, error) {
	return c.cfgMorph.client.DepositNotary(
		c.cfgMorph.notaryDepositAmount,
		c.cfgMorph.notaryDepositDuration+notaryDepositExtraBlocks,
	)
}

var (
	errNotaryDepositFail    = errors.New("notary deposit tx has faulted")
	errNotaryDepositTimeout = errors.New("notary deposit tx has not appeared in the network")
)

func waitNotaryDeposit(c *cfg, tx util.Uint256) error {
	for i := 0; i < notaryDepositRetriesAmount; i++ {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		ok, err := c.cfgMorph.client.TxHalt(tx)
		if err == nil {
			if ok {
				return nil
			}

			return errNotaryDepositFail
		}

		err = c.cfgMorph.client.Wait(c.ctx, 1)
		if err != nil {
			return fmt.Errorf("could not wait for one block in chain: %w", err)
		}
	}

	return errNotaryDepositTimeout
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

func registerNotificationHandlers(scHash util.Uint160, lis event.Listener, parsers map[event.Type]event.NotificationParser,
	subs map[event.Type][]event.Handler) {
	for typ, handlers := range subs {
		pi := event.NotificationParserInfo{}
		pi.SetType(typ)
		pi.SetScriptHash(scHash)

		p, ok := parsers[typ]
		if !ok {
			panic(fmt.Sprintf("missing parser for event %s", typ))
		}

		pi.SetParser(p)

		lis.SetNotificationParser(pi)

		for _, h := range handlers {
			hi := event.NotificationHandlerInfo{}
			hi.SetType(typ)
			hi.SetScriptHash(scHash)
			hi.SetHandler(h)

			lis.RegisterNotificationHandler(hi)
		}
	}
}

func registerBlockHandler(lis event.Listener, handler event.BlockHandler) {
	lis.RegisterBlockHandler(handler)
}

// lookupScriptHashesInNNS looks up for contract script hashes in NNS contract of side
// chain if they were not specified in config file.
func lookupScriptHashesInNNS(c *cfg) {
	var (
		err error

		emptyHash = util.Uint160{}
		targets   = [...]struct {
			h       *util.Uint160
			nnsName string
		}{
			{&c.cfgNetmap.scriptHash, client.NNSNetmapContractName},
			{&c.cfgAccounting.scriptHash, client.NNSBalanceContractName},
			{&c.cfgContainer.scriptHash, client.NNSContainerContractName},
			{&c.cfgReputation.scriptHash, client.NNSReputationContractName},
			{&c.cfgMorph.proxyScriptHash, client.NNSProxyContractName},
		}
	)

	for _, t := range targets {
		if emptyHash.Equals(*t.h) {
			*t.h, err = c.cfgMorph.client.NNSContractAddress(t.nnsName)
			fatalOnErrDetails(fmt.Sprintf("can't resolve %s in NNS", t.nnsName), err)
		}
	}
}
