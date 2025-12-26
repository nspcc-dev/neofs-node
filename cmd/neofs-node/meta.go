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
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sync/errgroup"
)

func initMeta(c *cfg) {
	//if c.cfgMorph.client == nil {
	//	initMorphComponents(c)
	//}
	//
	//c.cfgMeta.network = &neofsNetwork{
	//	key:        c.binPublicKey,
	//	cnrClient:  c.cCli,
	//	containers: c.cnrSrc,
	//	network:    c.netMapSource,
	//	header:     c.cfgObject.getSvc,
	//}
	//
	//var err error
	//p := meta.Parameters{
	//	Logger:        c.log.With(zap.String("service", "metadata chain")),
	//	Network:       c.cfgMeta.network,
	//	Timeout:       c.appCfg.FSChain.DialTimeout,
	//	NeoEnpoints:   c.appCfg.FSChain.Endpoints,
	//	ContainerHash: c.containerSH,
	//	NetmapHash:    c.netmapSH,
	//	RootPath:      c.appCfg.Meta.Path,
	//}
	//if p.RootPath == "" {
	//	p.RootPath = "metadata"
	//}
	//c.metaService, err = meta.New(p)
	//fatalOnErr(err)
	//
	//c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
	//	err = c.metaService.Run(ctx)
	//	if err != nil {
	//		c.internalErr <- fmt.Errorf("meta data service error: %w", err)
	//	}
	//}))
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

func (c *neofsNetwork) IsMineWithMeta(cData []byte) (bool, error) {
	curEpoch, err := c.network.Epoch()
	if err != nil {
		return false, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	networkMap, err := c.network.GetNetMapByEpoch(curEpoch)
	if err != nil {
		return false, fmt.Errorf("read network map at the current epoch #%d: %w", curEpoch, err)
	}
	var cnr containerSDK.Container
	err = cnr.Unmarshal(cData)
	if err != nil {
		return false, fmt.Errorf("unmarshal container: %w", err)
	}
	return c.isMineWithMeta(cnr, networkMap), nil
}

func (c *neofsNetwork) isMineWithMeta(cnr containerSDK.Container, networkMap *netmapsdk.NetMap) bool {
	const metaOnChainAttr = "__NEOFS__METAINFO_CONSISTENCY"
	switch cnr.Attribute(metaOnChainAttr) {
	case "optimistic", "strict":
	default:
		return false
	}

	return isContainerMine(cnr, networkMap, c.key)
}

func isContainerMine(cnr containerSDK.Container, networkMap *netmapsdk.NetMap, myKey []byte) bool {
	var id = cid.NewFromMarshalledContainer(cnr.Marshal())

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
				return err
			}

			if c.isMineWithMeta(cnr, networkMap) {
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
