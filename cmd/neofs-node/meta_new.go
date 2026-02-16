package main

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
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

	seedList, err := incPort(fsChainProtocol.SeedList)
	fatalOnErr(err)

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
			SeedList:                        seedList,
			StandbyCommittee:                standByCommittee,
			StateRootInHeader:               false,
			MaxTimePerBlock:                 20 * time.Second,
			ValidatorsCount:                 uint32(len(standByCommittee)),
			ValidatorsHistory:               nil,
			VerifyTransactions:              true,
		},
		ApplicationConfiguration: config.ApplicationConfiguration{
			P2P:               config.P2P{},
			Oracle:            config.OracleConfiguration{},
			P2PNotary:         config.P2PNotary{},
			StateRoot:         config.StateRoot{},
			NeoFSBlockFetcher: config.NeoFSBlockFetcher{},
			NeoFSStateFetcher: config.NeoFSStateFetcher{},
		},
	}

	var cfgDB = dbconfig.DBConfiguration{
		Type: dbconfig.BoltDB,
		BoltDBOptions: dbconfig.BoltDBOptions{
			FilePath: c.appCfg.Meta.Path,
		},
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
}

func incPort(addrs []string) ([]string, error) {
	res := slices.Clone(addrs)
	for i := range res {
		host, port, err := net.SplitHostPort(res[i])
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, res[i], err)
		}
		portU, err := strconv.ParseUint(port, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, port, err)
		}
		portU++
		res[i] = net.JoinHostPort(host, strconv.FormatUint(portU, 10))
	}

	return res, nil
}

func applyMetachainDefaults(cfg *config.Config) {
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
