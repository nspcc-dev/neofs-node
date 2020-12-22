package innerring

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
)

type ClientCache struct {
	cache *cache.ClientCache
	key   *ecdsa.PrivateKey
}

func newClientCache(key *ecdsa.PrivateKey) *ClientCache {
	return &ClientCache{
		cache: cache.NewSDKClientCache(),
		key:   key,
	}
}

func (c *ClientCache) Get(address string, opts ...client.Option) (*client.Client, error) {
	return c.cache.Get(c.key, address, opts...)
}

// GetSG polls the container from audit task to get the object by id.
// Returns storage groups structure from received object.
func (c *ClientCache) GetSG(task *audit.Task, id *object.ID) (*storagegroup.StorageGroup, error) {
	panic("implement me")
}

// GetHeader requests node from the container under audit to return object header by id.
func (c *ClientCache) GetHeader(task *audit.Task, node *netmap.Node, id *object.ID) (*object.Object, error) {
	panic("implement me")
}

// GetRangeHash requests node from the container under audit to return Tillich-Zemor hash of the
// payload range of the object with specified identifier.
func (c *ClientCache) GetRangeHash(task *audit.Task, node *netmap.Node, id *object.ID, rng *object.Range) ([]byte, error) {
	panic("implement me")
}
