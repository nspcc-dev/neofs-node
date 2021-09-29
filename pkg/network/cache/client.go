package cache

import (
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
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
func (c *ClientCache) Get(info clientcore.NodeInfo) (client.Client, error) {
	netAddr := info.AddressGroup()

	// multiaddr is used as a key in client cache since
	// same host may have different connections(with tls or not),
	// therefore, host+port pair is not unique

	// FIXME: we should calculate map key regardless of the address order,
	//  but network.StringifyGroup is order-dependent.
	//  This works until the same mixed group is transmitted
	//  (for a network map, it seems to be true).
	mAddr := network.StringifyGroup(netAddr)

	c.mu.RLock()
	if cli, ok := c.clients[mAddr]; ok {
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
	if cli, ok := c.clients[mAddr]; ok {
		return cli, nil
	}

	cli := newMultiClient(netAddr, c.opts)

	c.clients[mAddr] = cli

	return cli, nil
}

// CloseAll closes underlying connections of all cached clients.
//
// Ignores closing errors.
func (c *ClientCache) CloseAll() {
	c.mu.RLock()

	{
		for _, cl := range c.clients {
			con := cl.Conn()
			if con != nil {
				_ = con.Close()
			}
		}
	}

	c.mu.RUnlock()
}
