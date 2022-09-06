package cache

import (
	"crypto/ecdsa"
	"sync"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

type (
	// ClientCache is a structure around neofs-sdk-go/client to reuse
	// already created clients.
	ClientCache struct {
		mu      *sync.RWMutex
		clients map[string]*multiClient
		opts    ClientCacheOpts
	}

	ClientCacheOpts struct {
		DialTimeout      time.Duration
		StreamTimeout    time.Duration
		Key              *ecdsa.PrivateKey
		ResponseCallback func(client.ResponseMetaInfo) error
	}
)

// NewSDKClientCache creates instance of client cache.
// `opts` are used for new client creation.
func NewSDKClientCache(opts ClientCacheOpts) *ClientCache {
	return &ClientCache{
		mu:      new(sync.RWMutex),
		clients: make(map[string]*multiClient),
		opts:    opts,
	}
}

// Get function returns existing client or creates a new one.
func (c *ClientCache) Get(info clientcore.NodeInfo) (clientcore.Client, error) {
	netAddr := info.AddressGroup()
	cacheKey := string(info.PublicKey())

	c.mu.RLock()
	if cli, ok := c.clients[cacheKey]; ok {
		c.mu.RUnlock()
		cli.updateGroup(netAddr)
		return cli, nil
	}

	c.mu.RUnlock()
	// if client is not found in cache, then create a new one
	c.mu.Lock()
	defer c.mu.Unlock()

	// check once again if client is missing in cache, concurrent routine could
	// create client while this routine was locked on `c.mu.Lock()`.
	if cli, ok := c.clients[cacheKey]; ok {
		// No need to update address group as the client has just been created.
		return cli, nil
	}

	newClientOpts := c.opts
	newClientOpts.ResponseCallback = clientcore.AssertKeyResponseCallback(info.PublicKey())
	cli := newMultiClient(netAddr, newClientOpts)

	c.clients[cacheKey] = cli

	return cli, nil
}

// CloseAll closes underlying connections of all cached clients.
//
// Ignores closing errors.
func (c *ClientCache) CloseAll() {
	c.mu.RLock()

	{
		for _, cl := range c.clients {
			_ = cl.Close()
		}
	}

	c.mu.RUnlock()
}
