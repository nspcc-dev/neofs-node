package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"

	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
