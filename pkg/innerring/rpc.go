package innerring

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	storagegroup2 "github.com/nspcc-dev/neofs-node/pkg/core/storagegroup"
	neofsapiclient "github.com/nspcc-dev/neofs-node/pkg/innerring/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit/auditor"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	storagegroupsvc "github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"go.uber.org/zap"
)

type (
	ClientCache struct {
		log   *zap.Logger
		cache interface {
			Get(clientcore.NodeInfo) (clientcore.Client, error)
			CloseAll()
		}
		key *ecdsa.PrivateKey

		sgTimeout, headTimeout, rangeTimeout time.Duration
	}

	clientCacheParams struct {
		Log *zap.Logger
		Key *ecdsa.PrivateKey

		AllowExternal bool

		SGTimeout, HeadTimeout, RangeTimeout time.Duration

		Buffers *sync.Pool
	}
)

func newClientCache(p *clientCacheParams) *ClientCache {
	log := p.Log
	if log == nil {
		log = zap.NewNop()
	}

	return &ClientCache{
		log: p.Log,
		cache: cache.NewSDKClientCache(
			cache.ClientCacheOpts{
				AllowExternal: p.AllowExternal,
				Buffers:       p.Buffers,
				Logger:        log}),
		key:          p.Key,
		sgTimeout:    p.SGTimeout,
		headTimeout:  p.HeadTimeout,
		rangeTimeout: p.RangeTimeout,
	}
}

func (c *ClientCache) Get(info clientcore.NodeInfo) (clientcore.Client, error) {
	// Because cache is used by `ClientCache` exclusively,
	// client will always have valid key.
	return c.cache.Get(info)
}

// GetSG polls the container to get the object by id.
// Returns storage groups structure from received object.
//
// Returns an error of type apistatus.ObjectNotFound if storage group is missing.
func (c *ClientCache) GetSG(prm storagegroup2.GetSGPrm) (*storagegroup.StorageGroup, error) {
	var sgAddress oid.Address
	sgAddress.SetContainer(prm.CID)
	sgAddress.SetObject(prm.OID)

	return c.getSG(prm.Context, sgAddress, &prm.NetMap, prm.Container)
}

func (c *ClientCache) getSG(ctx context.Context, addr oid.Address, nm *netmap.NetMap, cn [][]netmap.NodeInfo) (*storagegroup.StorageGroup, error) {
	obj := addr.Object()

	nodes, err := placement.BuildObjectPlacement(nm, cn, &obj)
	if err != nil {
		return nil, fmt.Errorf("can't build object placement: %w", err)
	}

	var info clientcore.NodeInfo

	var getObjPrm neofsapiclient.GetObjectPrm
	getObjPrm.SetAddress(addr)

	for _, node := range placement.FlattenNodes(nodes) {
		err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(node))
		if err != nil {
			return nil, fmt.Errorf("parse client node info: %w", err)
		}

		cli, err := c.getWrappedClient(info)
		if err != nil {
			c.log.Warn("can't setup remote connection",
				zap.Error(err))

			continue
		}

		cctx, cancel := context.WithTimeout(ctx, c.sgTimeout)
		getObjPrm.SetContext(cctx)

		// NOTE: we use the function which does not verify object integrity (checksums, signature),
		// but it would be useful to do as part of a data audit.
		res, err := cli.GetObject(getObjPrm)

		cancel()

		if err != nil {
			c.log.Warn("can't get storage group object",
				zap.Error(err))

			continue
		}

		var sg storagegroup.StorageGroup

		err = storagegroup.ReadFromObject(&sg, *res.Object())
		if err != nil {
			return nil, fmt.Errorf("can't parse storage group from a object: %w", err)
		}

		return &sg, nil
	}

	var errNotFound apistatus.ObjectNotFound

	return nil, errNotFound
}

// GetHeader requests node from the container under audit to return object header by id.
func (c *ClientCache) GetHeader(prm auditor.GetHeaderPrm) (*object.Object, error) {
	var objAddress oid.Address
	objAddress.SetContainer(prm.CID)
	objAddress.SetObject(prm.OID)

	var info clientcore.NodeInfo

	err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(prm.Node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	cli, err := c.getWrappedClient(info)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", info.AddressGroup(), err)
	}

	cctx, cancel := context.WithTimeout(prm.Context, c.headTimeout)

	var obj *object.Object

	if prm.NodeIsRelay {
		obj, err = neofsapiclient.GetObjectHeaderFromContainer(cctx, cli, objAddress)
	} else {
		obj, err = neofsapiclient.GetRawObjectHeaderLocally(cctx, cli, objAddress)
	}

	cancel()

	if err != nil {
		return nil, fmt.Errorf("object head error: %w", err)
	}

	return obj, nil
}

// GetRangeHash requests node from the container under audit to return Tillich-Zemor hash of the
// payload range of the object with specified identifier.
func (c *ClientCache) GetRangeHash(prm auditor.GetRangeHashPrm) ([]byte, error) {
	var objAddress oid.Address
	objAddress.SetContainer(prm.CID)
	objAddress.SetObject(prm.OID)

	var info clientcore.NodeInfo

	err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(prm.Node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	cli, err := c.getWrappedClient(info)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", info.AddressGroup(), err)
	}

	cctx, cancel := context.WithTimeout(prm.Context, c.rangeTimeout)

	h, err := neofsapiclient.HashObjectRange(cctx, cli, objAddress, prm.Range)

	cancel()

	if err != nil {
		return nil, fmt.Errorf("object rangehash error: %w", err)
	}

	return h, nil
}

func (c *ClientCache) getWrappedClient(info clientcore.NodeInfo) (neofsapiclient.Client, error) {
	// can be also cached
	var cInternal neofsapiclient.Client

	cli, err := c.Get(info)
	if err != nil {
		return cInternal, fmt.Errorf("could not get API client from cache")
	}

	cInternal.WrapBasicClient(cli)
	cInternal.SetPrivateKey(c.key)

	return cInternal, nil
}

func (c ClientCache) ListSG(ctx context.Context, node clientcore.NodeInfo, cnr cid.ID, notExpiredAt uint64) ([]oid.ID, error) {
	cli, err := c.Get(node)
	if err != nil {
		return nil, fmt.Errorf("could not get API client from cache: %w", err)
	}

	fs := storagegroupsvc.SearchQuery()
	fs.AddFilter(object.AttributeExpirationEpoch, strconv.FormatUint(notExpiredAt, 10), object.MatchNumGE)
	var opts client.SearchObjectsOptions
	var cursor string
	var next []client.SearchResultItem
	var res []oid.ID
	for {
		next, cursor, err = cli.SearchObjects(ctx, cnr, fs, nil, cursor, (*neofsecdsa.Signer)(c.key), opts)
		if err != nil {
			return nil, fmt.Errorf("search objects RPC: %w", err)
		}
		res = slices.Grow(res, len(res)+len(next))
		for i := range next {
			res = append(res, next[i].ID)
		}
		if cursor == "" {
			return res, nil
		}
	}
}
