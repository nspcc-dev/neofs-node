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
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func initMeta(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	c.cfgMeta.cLister = &metaContainerListener{
		key:        c.binPublicKey,
		cnrClient:  c.basics.cCli,
		containers: c.cfgObject.cnrSource,
		network:    c.basics.netMapSource,
	}

	var err error
	p := meta.Parameters{
		Logger:          c.log.With(zap.String("service", "meta data")),
		ContainerLister: c.cfgMeta.cLister,
		Timeout:         c.applicationConfiguration.fsChain.dialTimeout,
		NeoEnpoints:     c.applicationConfiguration.fsChain.endpoints,
		ContainerHash:   c.basics.containerSH,
		NetmapHash:      c.basics.netmapSH,
		RootPath:        c.applicationConfiguration.metadata.path,
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

type metaContainerListener struct {
	key []byte

	cnrClient  *cntClient.Client
	containers container.Source
	network    netmap.Source

	m          sync.RWMutex
	prevCnrs   []cid.ID
	prevNetMap *netmapsdk.NetMap
	prevRes    map[cid.ID]struct{}
}

func (c *metaContainerListener) IsMineWithMeta(id cid.ID) (bool, error) {
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

func (c *metaContainerListener) isMineWithMeta(id cid.ID, networkMap *netmapsdk.NetMap) (bool, error) {
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

func (c *metaContainerListener) List() (map[cid.ID]struct{}, error) {
	actualContainers, err := c.cnrClient.List(nil)
	if err != nil {
		return nil, fmt.Errorf("read containers: %w", err)
	}
	curEpoch, err := c.network.Epoch()
	if err != nil {
		return nil, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	networkMap, err := c.network.GetNetMapByEpoch(curEpoch)
	if err != nil {
		return nil, fmt.Errorf("read network map at the current epoch #%d: %w", curEpoch, err)
	}

	c.m.RLock()
	if c.prevNetMap != nil && c.prevCnrs != nil {
		cnrsSame := slices.Equal(c.prevCnrs, actualContainers)
		if !cnrsSame {
			c.m.RUnlock()
			return c.prevRes, nil
		}
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
