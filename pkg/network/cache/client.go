package cache

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
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
func (c *ClientCache) Get(netAddr *network.Address) (client.Client, error) {
	hostAddr, err := netAddr.HostAddrString()
	if err != nil {
		return nil, fmt.Errorf("could not parse address as a string: %w", err)
	}

	c.mu.RLock()
	if cli, ok := c.clients[hostAddr]; ok {
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
	if cli, ok := c.clients[hostAddr]; ok {
		return cli, nil
	}

	opts := append(c.opts, client.WithAddress(hostAddr))

	cli, err := client.New(opts...)
	if err != nil {
		return nil, err
	}

	c.clients[hostAddr] = cli

	return cli, nil
}
