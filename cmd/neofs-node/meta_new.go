package main

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain/gas"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	meta "github.com/nspcc-dev/neofs-node/pkg/services/meta_new"
	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	"go.uber.org/zap"
)

func initMetaNew(c *cfg) {
	l := c.log.With(zap.String("component", "metadata chain (SN)"))

	v, err := c.cfgMorph.client.GetVersion()
	fatalOnErr(err)

	fsChainProtocol := v.Protocol
	standByCommittee := make([]string, 0, len(v.Protocol.StandbyCommittee))
	for _, c := range v.Protocol.StandbyCommittee {
		standByCommittee = append(standByCommittee, c.StringCompressed())
	}

	seedList, err := network.ChangePort(fsChainProtocol.SeedList, c.appCfg.Meta.SeedPort)
	if err != nil {
		fatalOnErr(fmt.Errorf("cannot apply new metadata chain seed ports to the FS chain ones: %w", err))
	}

	var chainCfg = config.Config{
		ProtocolConfiguration: config.ProtocolConfiguration{
			Genesis: config.Genesis{
				MaxTraceableBlocks:          fsChainProtocol.MaxTraceableBlocks,
				MaxValidUntilBlockIncrement: fsChainProtocol.MaxValidUntilBlockIncrement,
				Roles: map[noderoles.Role]keys.PublicKeys{
					noderoles.P2PNotary:     v.Protocol.StandbyCommittee,
					noderoles.NeoFSAlphabet: v.Protocol.StandbyCommittee,
				},
				TimePerBlock: 50 * time.Millisecond,
			},
			Magic:                           fsChainProtocol.Network + 1,
			Hardforks:                       nil,
			P2PNotaryRequestPayloadPoolSize: 1000,
			MaxTraceableBlocks:              fsChainProtocol.MaxTraceableBlocks,
			MaxValidUntilBlockIncrement:     fsChainProtocol.MaxValidUntilBlockIncrement,
			P2PSigExtensions:                true,
			SeedList:                        seedList,
			StandbyCommittee:                standByCommittee,
			TimePerBlock:                    50 * time.Millisecond,
			MaxTimePerBlock:                 20 * time.Second,
			ValidatorsCount:                 uint32(len(standByCommittee)),
			VerifyTransactions:              true,
		},
		ApplicationConfiguration: config.ApplicationConfiguration{
			P2P: config.P2P{
				Addresses:              c.appCfg.Meta.P2PAddresses,
				MinPeers:               len(seedList),
				BroadcastTxsBatchDelay: 5 * time.Millisecond,
				AttemptConnPeers:       100,
			},
			Relay: true,
		},
	}

	chainCfg.ApplicationConfiguration.DBConfiguration = dbconfig.DBConfiguration{
		Type: dbconfig.BoltDB,
		BoltDBOptions: dbconfig.BoltDBOptions{
			FilePath: path.Join(c.appCfg.Meta.Path, "chain_bolt.db"),
		},
	}

	if len(c.appCfg.Meta.RPCAddresses) > 0 {
		var rpcConfig config.RPC
		rpcConfig.BasicService = config.BasicService{
			Enabled:   true,
			Addresses: c.appCfg.Meta.RPCAddresses,
		}

		chainCfg.ApplicationConfiguration.RPC = rpcConfig
	}

	applyMetachainDefaults(&chainCfg)

	err = chainCfg.ProtocolConfiguration.Validate()
	fatalOnErr(err)

	ch, err := sidechain.New(chainCfg, l, c.internalErr)
	fatalOnErr(err)

	c.sidechain = ch

	c.workers = append(c.workers, &workerFromFunc{
		fn: func(ctx context.Context) {
			err := ch.Run(ctx)
			if err != nil {
				c.internalErr <- err
			}
		},
	})
	c.closers = append(c.closers, func() {
		c.sidechain.Stop()
	})

	c.metaService, err = meta.New(meta.Parameters{
		Logger: c.log.With(zap.String("component", "metadata service")),
		Chain:  c.sidechain,
		Path:   c.appCfg.Meta.Path,
		Network: &neofsNetwork{
			key:        c.binPublicKey,
			cnrClient:  c.cCli,
			containers: c.cnrSrc,
			network:    c.netMapSource,
			header:     c.cfgObject.getSvc,
		},
	})
	fatalOnErr(err)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		err = c.metaService.Run(ctx)
		if err != nil {
			c.internalErr <- fmt.Errorf("meta data service error: %w", err)
		}
	}))
}

func applyMetachainDefaults(cfg *config.Config) {
	appCfg := cfg.ApplicationConfiguration

	if appCfg.P2P.MaxPeers == 0 {
		appCfg.P2P.MaxPeers = 100
	}
	if appCfg.P2P.AttemptConnPeers == 0 {
		appCfg.P2P.AttemptConnPeers = appCfg.P2P.MinPeers + 10
	}
	if appCfg.P2P.DialTimeout == 0 {
		appCfg.P2P.DialTimeout = time.Minute
	}
	if appCfg.P2P.ProtoTickInterval == 0 {
		appCfg.P2P.ProtoTickInterval = 2 * time.Second
	}
	if appCfg.P2P.PingInterval == 0 {
		appCfg.P2P.PingInterval = 30 * time.Second
	}
	if appCfg.P2P.PingTimeout == 0 {
		appCfg.P2P.PingTimeout = time.Minute
	}
	if appCfg.RPC.MaxWebSocketClients == 0 {
		appCfg.RPC.MaxWebSocketClients = 64
	}
	if appCfg.RPC.SessionPoolSize == 0 {
		appCfg.RPC.SessionPoolSize = 20
	}
	if appCfg.RPC.MaxGasInvoke == 0 {
		appCfg.RPC.MaxGasInvoke = fixedn.Fixed8(gas.DefaultBalance)
	}

	protocolCfg := cfg.ProtocolConfiguration
	if protocolCfg.MaxTraceableBlocks == 0 {
		protocolCfg.MaxTraceableBlocks = 17280
	}
	if protocolCfg.MaxValidUntilBlockIncrement == 0 {
		protocolCfg.MaxValidUntilBlockIncrement = 8640
	}
}
