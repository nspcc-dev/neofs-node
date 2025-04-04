package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func initMeta(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	c.cfgMeta.network = &neofsNetwork{
		key:        c.binPublicKey,
		cnrClient:  c.basics.cCli,
		containers: c.cnrSrc,
		network:    c.basics.netMapSource,
		header:     c.cfgObject.getSvc,
	}

	var err error
	p := meta.Parameters{
		Logger:        c.log.With(zap.String("service", "meta data")),
		Network:       c.cfgMeta.network,
		Timeout:       c.applicationConfiguration.fsChain.dialTimeout,
		NeoEnpoints:   c.applicationConfiguration.fsChain.endpoints,
		ContainerHash: c.basics.containerSH,
		NetmapHash:    c.basics.netmapSH,
		RootPath:      c.applicationConfiguration.metadata.path,
	}
	if p.RootPath == "" {
		p.RootPath = "metadata"
	}
	c.shared.metaService, err = meta.New(p)
	fatalOnErr(err)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		err = c.shared.metaService.Run(ctx)
		if err != nil {
			c.internalErr <- fmt.Errorf("meta data service error: %w", err)
		}
	}))
}

type neofsNetwork struct {
	key []byte

	cnrClient  *cntClient.Client
	containers container.Source
	network    netmap.Source
	header     *getsvc.Service

	m          sync.RWMutex
	prevCnrs   []cid.ID
	prevNetMap *netmapsdk.NetMap
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

func (c *neofsNetwork) IsMineWithMeta(id cid.ID) (bool, error) {
	curEpoch, err := c.network.Epoch()
	if err != nil {
		return false, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	networkMap, err := c.network.GetNetMapByEpoch(curEpoch)
	if err != nil {
		return false, fmt.Errorf("read network map at the current epoch #%d: %w", curEpoch, err)
	}
	return c.isMineWithMeta(id, networkMap)
}

func (c *neofsNetwork) isMineWithMeta(id cid.ID, networkMap *netmapsdk.NetMap) (bool, error) {
	cnr, err := c.containers.Get(id)
	if err != nil {
		return false, fmt.Errorf("read %s container: %w", id, err)
	}

	const metaOnChainAttr = "__NEOFS__METAINFO_CONSISTENCY"
	switch cnr.Value.Attribute(metaOnChainAttr) {
	case "optimistic", "strict":
	default:
		return false, nil
	}

	nodeSets, err := networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), id)
	if err != nil {
		return false, fmt.Errorf("apply container storage policy to %s container: %w", id, err)
	}

	for _, nodeSet := range nodeSets {
		for _, node := range nodeSet {
			if bytes.Equal(node.PublicKey(), c.key) {
				return true, nil
			}
		}
	}

	return false, nil
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
		netmapSame := slices.EqualFunc(c.prevNetMap.Nodes(), networkMap.Nodes(), func(n1 netmapsdk.NodeInfo, n2 netmapsdk.NodeInfo) bool {
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
			ok, err := c.isMineWithMeta(cID, networkMap)
			if err != nil || !ok {
				return err
			}

			locM.Lock()
			res[cID] = struct{}{}
			locM.Unlock()

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
