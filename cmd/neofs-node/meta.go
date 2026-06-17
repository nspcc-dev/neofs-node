package main

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain/gas"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type neofsNetwork struct {
	key []byte

	cnrClient  *cntClient.Client
	containers containercore.Source
	network    netmapcore.Source
	header     *getsvc.Service

	m          sync.RWMutex
	prevCnrs   []cid.ID
	prevNetMap *netmap.NetMap
	prevRes    map[cid.ID]struct{}
}

func (c *neofsNetwork) Epoch() (uint64, error) {
	return c.network.Epoch()
}

func (c *neofsNetwork) Head(ctx context.Context, cID cid.ID, oID oid.ID) (object.Object, error) {
	var hw headerWriter
	var hPrm getsvc.HeadPrm
	hPrm.SetHeaderWriter(&hw)
	hPrm.WithAddress(oid.NewAddress(cID, oID))

	err := c.header.Head(ctx, hPrm)
	if err != nil {
		return object.Object{}, err
	}

	return *hw.h, nil
}

func (c *neofsNetwork) IsMineWithMeta(id cid.ID, _ []byte) (bool, error) {
	curEpoch, err := c.network.Epoch()
	if err != nil {
		return false, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	networkMap, err := c.network.GetNetMapByEpoch(curEpoch)
	if err != nil {
		return false, fmt.Errorf("read network map at the current epoch #%d: %w", curEpoch, err)
	}
	cnr, err := c.containers.Get(id)
	if err != nil {
		return false, fmt.Errorf("get container: %w", err)
	}
	return c.isMineWithMeta(id, cnr, networkMap), nil
}

func (c *neofsNetwork) isMineWithMeta(id cid.ID, cnr container.Container, networkMap *netmap.NetMap) bool {
	const metaOnChainAttr = "__NEOFS__METAINFO_CONSISTENCY"
	switch cnr.Attribute(metaOnChainAttr) {
	case "optimistic", "strict":
	default:
		return false
	}

	return isContainerMine(id, cnr, networkMap, c.key)
}

func isContainerMine(id cid.ID, cnr container.Container, networkMap *netmap.NetMap, myKey []byte) bool {
	nodeSets, err := networkMap.ContainerNodes(cnr.PlacementPolicy(), id)
	if err != nil {
		return false
	}

	for _, nodeSet := range nodeSets {
		for _, node := range nodeSet {
			if bytes.Equal(node.PublicKey(), myKey) {
				return true
			}
		}
	}

	return false
}

func (c *neofsNetwork) List(e uint64) (map[cid.ID]struct{}, error) {
	actualContainers, err := c.cnrClient.List(nil)
	if err != nil {
		return nil, fmt.Errorf("read containers: %w", err)
	}
	networkMap, err := c.network.GetNetMapByEpoch(e)
	if err != nil {
		return nil, fmt.Errorf("read network map at the epoch #%d: %w", e, err)
	}

	c.m.RLock()
	if c.prevNetMap != nil && c.prevCnrs != nil && slices.Equal(c.prevCnrs, actualContainers) {
		netmapSame := slices.EqualFunc(c.prevNetMap.Nodes(), networkMap.Nodes(), func(n1 netmap.NodeInfo, n2 netmap.NodeInfo) bool {
			return bytes.Equal(n1.PublicKey(), n2.PublicKey())
		})
		if netmapSame {
			c.m.RUnlock()
			return c.prevRes, nil
		}
	}
	c.m.RUnlock()

	var locM sync.Mutex
	res := make(map[cid.ID]struct{})
	var wg errgroup.Group
	for _, cID := range actualContainers {
		wg.Go(func() error {
			cnr, err := c.containers.Get(cID)
			if err != nil {
				return err
			}

			if c.isMineWithMeta(cID, cnr, networkMap) {
				locM.Lock()
				res[cID] = struct{}{}
				locM.Unlock()
			}

			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return nil, err
	}

	c.m.Lock()
	c.prevCnrs = actualContainers
	c.prevNetMap = networkMap
	c.prevRes = res
	c.m.Unlock()

	return res, nil
}

func initMeta(c *cfg) {
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
			Hardforks:                       metadataChainHardforks(),
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
			RPC: config.RPC{
				DirectRelay: true,
			},
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

func metadataChainHardforks() map[string]uint32 {
	return map[string]uint32{
		config.HFAspidochelone.String(): 0,
		config.HFBasilisk.String():      0,
		config.HFCockatrice.String():    0,
		config.HFDomovoi.String():       0,
		config.HFEchidna.String():       0,
		config.HFFaun.String():          0,
	}
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
