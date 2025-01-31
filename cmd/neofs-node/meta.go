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

	c.cfgMeta.cLister = &containerListener{
		key:        c.binPublicKey,
		cnrClient:  c.basics.cCli,
		containers: c.cfgObject.cnrSource,
		network:    c.basics.netMapSource,
	}

	m, err := meta.New(c.log.With(zap.String("service", "meta data")),
		c.cfgMeta.cLister,
		c.applicationConfiguration.fsChain.dialTimeout,
		c.applicationConfiguration.fsChain.endpoints,
		c.basics.containerSH,
		c.basics.netmapSH,
		c.applicationConfiguration.metadata.path)
	fatalOnErr(err)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		err = m.Run(ctx)
		if err != nil {
			c.internalErr <- fmt.Errorf("meta data service error: %w", err)
		}
	}))
}

type containerListener struct {
	key []byte

	cnrClient  *cntClient.Client
	containers container.Source
	network    netmap.Source

	m          sync.RWMutex
	prevCnrs   []cid.ID
	prevNetMap *netmapsdk.NetMap
	prevRes    map[cid.ID]struct{}
}

func (c *containerListener) List() (map[cid.ID]struct{}, error) {
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
			cnr, err := c.containers.Get(cID)
			if err != nil {
				return fmt.Errorf("read %s container: %w", cID, err)
			}

			nodeSets, err := networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), cID)
			if err != nil {
				return fmt.Errorf("apply container storage policy to %s container: %w", cID, err)
			}

			for _, nodeSet := range nodeSets {
				for _, node := range nodeSet {
					if bytes.Equal(node.PublicKey(), c.key) {
						locM.Lock()
						res[cID] = struct{}{}
						locM.Unlock()
						return nil
					}
				}
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
