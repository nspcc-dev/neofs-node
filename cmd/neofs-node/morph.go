package main

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"go.uber.org/zap"
)

const (
	newEpochNotification = "NewEpoch"
)

func initMorphComponents(c *cfg) {
	var err error

	morphCli := c.shared.basics.cli
	c.cfgMorph.client = morphCli

	c.onShutdown(morphCli.Close)

	err = morphCli.EnableNotarySupport(
		client.WithProxyContract(
			c.cfgMorph.proxyScriptHash,
		),
	)
	fatalOnErr(err)
}

func initNotary(c *cfg) {
	fatalOnErr(makeNotaryDeposit(c))
}

func makeNotaryDeposit(c *cfg) error {
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
		return fmt.Errorf("could not calculate notary deposit: %w", err)
	}

	c.log.Debug("making neofs chain notary deposit", zap.Stringer("fixed8 deposit", depositAmount))

	return c.cfgMorph.client.DepositEndlessNotary(depositAmount)
}

func listenMorphNotifications(c *cfg) {
	lis, err := event.NewListener(event.ListenerParams{
		Logger: c.log,
		Client: c.cfgMorph.client,
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
			c.log.Info("new epoch event from FS chain",
				zap.Uint64("number", res.(netmapEvent.NewEpoch).EpochNumber()),
			)
		}

		return res, err
	})
	registerNotificationHandlers(c.shared.basics.netmapSH, lis, c.cfgNetmap.parsers, c.cfgNetmap.subscribers)
	registerNotificationHandlers(c.shared.basics.containerSH, lis, c.cfgContainer.parsers, c.cfgContainer.subscribers)

	lis.RegisterHeaderHandler(func(block *block.Header) {
		c.log.Debug("new block", zap.Uint32("index", block.Index))

		c.networkState.block.Store(block.Index)

		err = c.persistate.SetUInt32(persistateFSChainLastBlockKey, block.Index)
		if err != nil {
			c.log.Warn("can't update persistent state",
				zap.String("chain", "FS"),
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

// lookupScriptHashesInNNS looks up for contract script hashes in NNS contract of side chain.
func lookupScriptHashesInNNS(morphCli *client.Client, b *basics) {
	var (
		err error

		targets = [...]struct {
			finalHash *util.Uint160
			nnsName   string
		}{
			{&b.balanceSH, client.NNSBalanceContractName},
			{&b.containerSH, client.NNSContainerContractName},
			{&b.netmapSH, client.NNSNetmapContractName},
			{&b.reputationSH, client.NNSReputationContractName},
			{&b.proxySH, client.NNSProxyContractName},
		}
	)

	for _, t := range targets {
		*t.finalHash, err = morphCli.NNSContractAddress(t.nnsName)
		fatalOnErrDetails(fmt.Sprintf("can't resolve %s in NNS", t.nnsName), err)
	}
}
