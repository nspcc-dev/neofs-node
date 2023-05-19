package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	morphconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/morph"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"go.uber.org/zap"
)

const (
	newEpochNotification = "NewEpoch"

	// amount of tries(blocks) before notary deposit timeout.
	notaryDepositRetriesAmount = 300
)

func initMorphComponents(c *cfg) {
	var err error

	addresses := morphconfig.Endpoints(c.appCfg)

	cli, err := client.New(c.key,
		client.WithDialTimeout(morphconfig.DialTimeout(c.appCfg)),
		client.WithLogger(c.log),
		client.WithEndpoints(addresses),
		client.WithConnLostCallback(func() {
			c.internalErr <- errors.New("morph connection has been lost")
		}),
	)
	if err != nil {
		c.log.Info("failed to create neo RPC client",
			zap.Any("endpoints", addresses),
			zap.String("error", err.Error()),
		)

		fatalOnErr(err)
	}

	c.onShutdown(cli.Close)

	if err := cli.SetGroupSignerScope(); err != nil {
		c.log.Info("failed to set group signer scope, continue with Global", zap.Error(err))
	}

	c.cfgMorph.client = cli

	lookupScriptHashesInNNS(c) // smart contract auto negotiation

	err = c.cfgMorph.client.EnableNotarySupport(
		client.WithProxyContract(
			c.cfgMorph.proxyScriptHash,
		),
	)
	fatalOnErr(err)

	wrap, err := nmClient.NewFromMorph(c.cfgMorph.client, c.cfgNetmap.scriptHash, 0)
	fatalOnErr(err)

	var netmapSource netmap.Source

	c.cfgMorph.cacheTTL = morphconfig.CacheTTL(c.appCfg)

	if c.cfgMorph.cacheTTL == 0 {
		msPerBlock, err := c.cfgMorph.client.MsPerBlock()
		fatalOnErr(err)
		c.cfgMorph.cacheTTL = time.Duration(msPerBlock) * time.Millisecond
		c.log.Debug("morph.cache_ttl fetched from network", zap.Duration("value", c.cfgMorph.cacheTTL))
	}

	if c.cfgMorph.cacheTTL < 0 {
		netmapSource = wrap
	} else {
		// use RPC node as source of netmap (with caching)
		netmapSource = newCachedNetmapStorage(c.cfgNetmap.state, wrap)
	}

	c.netMapSource = netmapSource
	c.cfgNetmap.wrapper = wrap
}

func makeAndWaitNotaryDeposit(c *cfg) {
	tx, err := makeNotaryDeposit(c)
	fatalOnErr(err)

	err = waitNotaryDeposit(c, tx)
	fatalOnErr(err)
}

func makeNotaryDeposit(c *cfg) (util.Uint256, error) {
	const (
		// gasMultiplier defines how many times more the notary
		// balance must be compared to the GAS balance of the node:
		//     notaryBalance = GASBalance * gasMultiplier
		gasMultiplier = 3

		// gasDivisor defines what part of GAS balance (1/gasDivisor)
		// should be transferred to the notary service
		gasDivisor = 2
	)

	depositAmount, err := client.CalculateNotaryDepositAmount(c.cfgMorph.client, gasMultiplier, gasDivisor)
	if err != nil {
		return util.Uint256{}, fmt.Errorf("could not calculate notary deposit: %w", err)
	}

	return c.cfgMorph.client.DepositEndlessNotary(depositAmount)
}

var (
	errNotaryDepositFail    = errors.New("notary deposit tx has faulted")
	errNotaryDepositTimeout = errors.New("notary deposit tx has not appeared in the network")
)

func waitNotaryDeposit(c *cfg, tx util.Uint256) error {
	for i := 0; i < notaryDepositRetriesAmount; i++ {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
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
	// listenerPoolCap is a capacity of a
	// worker pool inside the listener. It
	// is used to prevent blocking in neo-go:
	// the client cannot make RPC requests if
	// the notification channel is not being
	// read by another goroutine.
	const listenerPoolCap = 10

	var (
		err  error
		subs subscriber.Subscriber
	)

	fromSideChainBlock, err := c.persistate.UInt32(persistateSideChainLastBlockKey)
	if err != nil {
		fromSideChainBlock = 0
		c.log.Warn("can't get last processed side chain block number", zap.String("error", err.Error()))
	}

	subs, err = subscriber.New(c.ctx, &subscriber.Params{
		Log:            c.log,
		StartFromBlock: fromSideChainBlock,
		Client:         c.cfgMorph.client,
	})
	fatalOnErr(err)

	lis, err := event.NewListener(event.ListenerParams{
		Logger:             c.log,
		Subscriber:         subs,
		WorkerPoolCapacity: listenerPoolCap,
	})
	fatalOnErr(err)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		runAndLog(c, "morph notification", false, func(c *cfg) {
			lis.ListenWithError(ctx, c.internalErr)
		})
	}))

	setNetmapNotificationParser(c, newEpochNotification, func(src *state.ContainedNotificationEvent) (event.Event, error) {
		res, err := netmapEvent.ParseNewEpoch(src)
		if err == nil {
			c.log.Info("new epoch event from sidechain",
				zap.Uint64("number", res.(netmapEvent.NewEpoch).EpochNumber()),
			)
		}

		return res, err
	})
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
