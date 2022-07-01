package innerring

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	neofsapiclient "github.com/nspcc-dev/neofs-node/pkg/innerring/internal/client"
	auditproc "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

		SGTimeout, HeadTimeout, RangeTimeout time.Duration
	}
)

func newClientCache(p *clientCacheParams) *ClientCache {
	return &ClientCache{
		log:          p.Log,
		cache:        cache.NewSDKClientCache(cache.ClientCacheOpts{}),
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

// GetSG polls the container from audit task to get the object by id.
// Returns storage groups structure from received object.
//
// Returns an error of type apistatus.ObjectNotFound if storage group is missing.
func (c *ClientCache) GetSG(task *audit.Task, id oid.ID) (*storagegroup.StorageGroup, error) {
	var sgAddress oid.Address
	sgAddress.SetContainer(task.ContainerID())
	sgAddress.SetObject(id)

	return c.getSG(task.AuditContext(), sgAddress, task.NetworkMap(), task.ContainerNodes())
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
				zap.String("error", err.Error()))

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
				zap.String("error", err.Error()))

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
func (c *ClientCache) GetHeader(task *audit.Task, node netmap.NodeInfo, id oid.ID, relay bool) (*object.Object, error) {
	var objAddress oid.Address
	objAddress.SetContainer(task.ContainerID())
	objAddress.SetObject(id)

	var info clientcore.NodeInfo

	err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	cli, err := c.getWrappedClient(info)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", info.AddressGroup(), err)
	}

	cctx, cancel := context.WithTimeout(task.AuditContext(), c.headTimeout)

	var obj *object.Object

	if relay {
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
func (c *ClientCache) GetRangeHash(task *audit.Task, node netmap.NodeInfo, id oid.ID, rng *object.Range) ([]byte, error) {
	var objAddress oid.Address
	objAddress.SetContainer(task.ContainerID())
	objAddress.SetObject(id)

	var info clientcore.NodeInfo

	err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(node))
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	cli, err := c.getWrappedClient(info)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", info.AddressGroup(), err)
	}

	cctx, cancel := context.WithTimeout(task.AuditContext(), c.rangeTimeout)

	h, err := neofsapiclient.HashObjectRange(cctx, cli, objAddress, rng)

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

func (c ClientCache) ListSG(dst *auditproc.SearchSGDst, prm auditproc.SearchSGPrm) error {
	cli, err := c.getWrappedClient(prm.NodeInfo())
	if err != nil {
		return fmt.Errorf("could not get API client from cache")
	}

	var cliPrm neofsapiclient.SearchSGPrm

	cliPrm.SetContext(prm.Context())
	cliPrm.SetContainerID(prm.CID())

	res, err := cli.SearchSG(cliPrm)
	if err != nil {
		return err
	}

	dst.WriteIDList(res.IDList())

	return nil
}
