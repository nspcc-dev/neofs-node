package innerring

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
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
