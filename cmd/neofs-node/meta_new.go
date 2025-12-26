package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	"go.uber.org/zap"
)

func initMeta_new(c *cfg) {
	l := c.log.With(zap.String("component", "metadata chain"))

	v, err := c.cfgMorph.client.GetVersion()
	fatalOnErr(err)

	fsChainProtocol := v.Protocol
	standByCommittee := make([]string, 0, len(v.Protocol.StandbyCommittee))
	for _, c := range v.Protocol.StandbyCommittee {
		standByCommittee = append(standByCommittee, c.StringCompressed())
	}

	p2pCfg := c.appCfg.Meta.P2P

	var chainCfg = config.Config{
		ProtocolConfiguration: config.ProtocolConfiguration{
			CommitteeHistory: nil,
			Genesis: config.Genesis{
				MaxTraceableBlocks:          fsChainProtocol.MaxTraceableBlocks,
				MaxValidUntilBlockIncrement: fsChainProtocol.MaxValidUntilBlockIncrement,
				Roles: map[noderoles.Role]keys.PublicKeys{
					noderoles.P2PNotary:     v.Protocol.StandbyCommittee,
					noderoles.NeoFSAlphabet: v.Protocol.StandbyCommittee,
				},
				TimePerBlock: time.Duration(fsChainProtocol.MillisecondsPerBlock) * time.Millisecond,
			},
			Magic:                           fsChainProtocol.Network + 1,
			InitialGASSupply:                fsChainProtocol.InitialGasDistribution,
			P2PNotaryRequestPayloadPoolSize: 1000,
			MaxTraceableBlocks:              fsChainProtocol.MaxTraceableBlocks,
			MaxValidUntilBlockIncrement:     fsChainProtocol.MaxValidUntilBlockIncrement,
			P2PSigExtensions:                true,
			SeedList:                        c.appCfg.Meta.SeedNodes,
			StandbyCommittee:                standByCommittee,
			StateRootInHeader:               false,
			MaxTimePerBlock:                 c.appCfg.Meta.MaxTimePerBlock,
			ValidatorsCount:                 uint32(len(standByCommittee)),
			ValidatorsHistory:               nil,
			VerifyTransactions:              true,
		},
		ApplicationConfiguration: config.ApplicationConfiguration{
			P2P: config.P2P{
				Addresses:         p2pCfg.Listen,
				AttemptConnPeers:  int(p2pCfg.Peers.Attempts),
				DialTimeout:       p2pCfg.DialTimeout,
				MaxPeers:          int(p2pCfg.Peers.Max),
				MinPeers:          int(p2pCfg.Peers.Min),
				PingInterval:      p2pCfg.Ping.Interval,
				PingTimeout:       p2pCfg.Ping.Timeout,
				ProtoTickInterval: p2pCfg.ProtoTickInterval,
			},
			Oracle:            config.OracleConfiguration{},
			P2PNotary:         config.P2PNotary{},
			StateRoot:         config.StateRoot{},
			NeoFSBlockFetcher: config.NeoFSBlockFetcher{},
			NeoFSStateFetcher: config.NeoFSStateFetcher{},
		},
	}

	var cfgDB dbconfig.DBConfiguration
	cfgDB.Type = c.appCfg.Meta.Storage.Type
	switch c.appCfg.Meta.Storage.Type {
	case dbconfig.BoltDB:
		cfgDB.BoltDBOptions.FilePath = c.appCfg.Meta.Storage.Path
	case dbconfig.LevelDB:
		cfgDB.LevelDBOptions.DataDirectoryPath = c.appCfg.Meta.Storage.Path
	default:
		panic(fmt.Sprintf("unsupported metadata storage type: %s", c.appCfg.Meta.Storage.Type))
	}
	chainCfg.ApplicationConfiguration.DBConfiguration = cfgDB

	if len(c.appCfg.Meta.RPC.Listen) > 0 {
		var (
			rpcConfig  config.RPC
			rpcCfgRead = c.appCfg.Meta.RPC
		)

		rpcConfig.BasicService = config.BasicService{
			Enabled:   true,
			Addresses: c.appCfg.Meta.RPC.Listen,
		}
		rpcConfig.MaxGasInvoke = fixedn.Fixed8FromInt64(int64(rpcCfgRead.MaxGasInvoke))
		rpcConfig.MaxIteratorResultItems = 100
		rpcConfig.MaxWebSocketClients = int(rpcCfgRead.MaxWebSocketClients)
		rpcConfig.SessionEnabled = true
		rpcConfig.SessionExpansionEnabled = true
		rpcConfig.SessionPoolSize = int(rpcCfgRead.SessionPoolSize)
		rpcConfig.StartWhenSynchronized = true
		if tlsCfg := rpcCfgRead.TLS; tlsCfg.Enabled {
			rpcConfig.TLSConfig.Enabled = true
			rpcConfig.TLSConfig.Addresses = tlsCfg.Listen
			rpcConfig.TLSConfig.CertFile = tlsCfg.CertFile
			rpcConfig.TLSConfig.KeyFile = tlsCfg.KeyFile
		}

		chainCfg.ApplicationConfiguration.RPC = rpcConfig
	}

	applySidechainDefaults(&chainCfg)

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
}

func applySidechainDefaults(cfg *config.Config) {
	if cfg.ApplicationConfiguration.P2P.MaxPeers == 0 {
		cfg.ApplicationConfiguration.P2P.MaxPeers = 100
	}
	if cfg.ApplicationConfiguration.P2P.AttemptConnPeers == 0 {
		cfg.ApplicationConfiguration.P2P.AttemptConnPeers = cfg.ApplicationConfiguration.P2P.MinPeers + 10
	}
	if cfg.ApplicationConfiguration.P2P.DialTimeout == 0 {
		cfg.ApplicationConfiguration.P2P.DialTimeout = time.Minute
	}
	if cfg.ApplicationConfiguration.P2P.ProtoTickInterval == 0 {
		cfg.ApplicationConfiguration.P2P.ProtoTickInterval = 2 * time.Second
	}
	if cfg.ProtocolConfiguration.MaxTraceableBlocks == 0 {
		cfg.ProtocolConfiguration.MaxTraceableBlocks = 17280
	}
	if cfg.ProtocolConfiguration.MaxValidUntilBlockIncrement == 0 {
		cfg.ProtocolConfiguration.MaxValidUntilBlockIncrement = 8640
	}
	if cfg.ApplicationConfiguration.P2P.PingInterval == 0 {
		cfg.ApplicationConfiguration.P2P.PingInterval = 30 * time.Second
	}
	if cfg.ApplicationConfiguration.P2P.PingTimeout == 0 {
		cfg.ApplicationConfiguration.P2P.PingTimeout = time.Minute
	}
	if cfg.ApplicationConfiguration.RPC.MaxWebSocketClients == 0 {
		cfg.ApplicationConfiguration.RPC.MaxWebSocketClients = 64
	}
	if cfg.ApplicationConfiguration.RPC.SessionPoolSize == 0 {
		cfg.ApplicationConfiguration.RPC.SessionPoolSize = 20
	}
	if cfg.ApplicationConfiguration.RPC.MaxGasInvoke == 0 {
		cfg.ApplicationConfiguration.RPC.MaxGasInvoke = 100
	}
}
