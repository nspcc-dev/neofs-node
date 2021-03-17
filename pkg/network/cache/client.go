package cache

import (
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
)

type (
	// ClientCache is a structure around neofs-api-go/pkg/client to reuse
	// already created clients.
	ClientCache struct {
		mu      *sync.RWMutex
		clients map[string]client.Client
		opts    []client.Option
	}
)

// NewSDKClientCache creates instance of client cache.
// `opts` are used for new client creation.
func NewSDKClientCache(opts ...client.Option) *ClientCache {
	return &ClientCache{
		mu:      new(sync.RWMutex),
		clients: make(map[string]client.Client),
		opts:    opts,
	}
}

// Get function returns existing client or creates a new one.
func (c *ClientCache) Get(address string) (client.Client, error) {
	c.mu.RLock()
	if cli, ok := c.clients[address]; ok {
		// todo: check underlying connection neofs-api-go#196
		c.mu.RUnlock()

		return cli, nil
	}

	c.mu.RUnlock()
	// if client is not found in cache, then create a new one
	c.mu.Lock()
	defer c.mu.Unlock()

	// check once again if client is missing in cache, concurrent routine could
	// create client while this routine was locked on `c.mu.Lock()`.
	if cli, ok := c.clients[address]; ok {
		return cli, nil
	}

	cli, err := client.New(append(c.opts, client.WithAddress(address))...)
	if err != nil {
		return nil, err
	}

	c.clients[address] = cli

	return cli, nil
}
