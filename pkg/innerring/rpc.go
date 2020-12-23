package innerring

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	coreObject "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"go.uber.org/zap"
)

type (
	ClientCache struct {
		log   *zap.Logger
		cache *cache.ClientCache
		key   *ecdsa.PrivateKey

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
		cache:        cache.NewSDKClientCache(),
		key:          p.Key,
		sgTimeout:    p.SGTimeout,
		headTimeout:  p.HeadTimeout,
		rangeTimeout: p.RangeTimeout,
	}
}

func (c *ClientCache) Get(address string, opts ...client.Option) (*client.Client, error) {
	return c.cache.Get(c.key, address, opts...)
}

// GetSG polls the container from audit task to get the object by id.
// Returns storage groups structure from received object.
func (c *ClientCache) GetSG(task *audit.Task, id *object.ID) (*storagegroup.StorageGroup, error) {
	nodes, err := placement.BuildObjectPlacement( // shuffle nodes
		task.NetworkMap(),
		task.ContainerNodes(),
		id,
	)
	if err != nil {
		return nil, fmt.Errorf("can't build object placement: %w", err)
	}

	sgAddress := new(object.Address)
	sgAddress.SetContainerID(task.ContainerID())
	sgAddress.SetObjectID(id)

	getParams := new(client.GetObjectParams)
	getParams.WithAddress(sgAddress)

	for _, node := range placement.FlattenNodes(nodes) {
		addr, err := network.IPAddrFromMultiaddr(node.Address())
		if err != nil {
			c.log.Warn("can't parse remote address",
				zap.String("address", node.Address()),
				zap.String("error", err.Error()))

			continue
		}

		cli, err := c.Get(addr)
		if err != nil {
			c.log.Warn("can't setup remote connection",
				zap.String("address", addr),
				zap.String("error", err.Error()))

			continue
		}

		cctx, cancel := context.WithTimeout(task.AuditContext(), c.sgTimeout)
		obj, err := cli.GetObject(cctx, getParams)

		cancel()

		if err != nil {
			c.log.Warn("can't get storage group object",
				zap.String("error", err.Error()))

			continue
		}

		sg := storagegroup.New()

		err = sg.Unmarshal(obj.Payload())
		if err != nil {
			return nil, fmt.Errorf("can't parse storage group payload: %w", err)
		}

		return sg, nil
	}

	return nil, coreObject.ErrNotFound
}

// GetHeader requests node from the container under audit to return object header by id.
func (c *ClientCache) GetHeader(task *audit.Task, node *netmap.Node, id *object.ID, relay bool) (*object.Object, error) {
	raw := true
	ttl := uint32(1)

	if relay {
		ttl = 10 // todo: instead of hardcode value we can set TTL based on container length
		raw = false
	}

	objAddress := new(object.Address)
	objAddress.SetContainerID(task.ContainerID())
	objAddress.SetObjectID(id)

	headParams := new(client.ObjectHeaderParams)
	headParams.WithRawFlag(raw)
	headParams.WithMainFields()
	headParams.WithAddress(objAddress)

	addr, err := network.IPAddrFromMultiaddr(node.Address())
	if err != nil {
		return nil, fmt.Errorf("can't parse remote address %s: %w", node.Address(), err)
	}

	cli, err := c.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", addr, err)
	}

	cctx, cancel := context.WithTimeout(task.AuditContext(), c.headTimeout)
	head, err := cli.GetObjectHeader(cctx, headParams, client.WithTTL(ttl))

	cancel()

	if err != nil {
		return nil, fmt.Errorf("object head error: %w", err)
	}

	return head, nil
}

// GetRangeHash requests node from the container under audit to return Tillich-Zemor hash of the
// payload range of the object with specified identifier.
func (c *ClientCache) GetRangeHash(task *audit.Task, node *netmap.Node, id *object.ID, rng *object.Range) ([]byte, error) {
	objAddress := new(object.Address)
	objAddress.SetContainerID(task.ContainerID())
	objAddress.SetObjectID(id)

	rangeParams := new(client.RangeChecksumParams)
	rangeParams.WithAddress(objAddress)
	rangeParams.WithRangeList(rng)
	rangeParams.WithSalt(nil) // it MUST be nil for correct hash concatenation in PDP game

	addr, err := network.IPAddrFromMultiaddr(node.Address())
	if err != nil {
		return nil, fmt.Errorf("can't parse remote address %s: %w", node.Address(), err)
	}

	cli, err := c.Get(addr)
	if err != nil {
		return nil, fmt.Errorf("can't setup remote connection with %s: %w", addr, err)
	}

	cctx, cancel := context.WithTimeout(task.AuditContext(), c.rangeTimeout)
	result, err := cli.ObjectPayloadRangeTZ(cctx, rangeParams, client.WithTTL(1))

	cancel()

	if err != nil {
		return nil, fmt.Errorf("object rangehash error: %w", err)
	}

	// client guarantees that request and response have equal amount of ranges

	return result[0][:], nil
}
