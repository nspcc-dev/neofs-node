package cache

import (
	"crypto/tls"
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
	// multiaddr is used as a key in client cache since
	// same host may have different connections(with tls or not),
	// therefore, host+port pair is not unique
	mAddr := netAddr.String()

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

	hostAddr, err := netAddr.HostAddrString()
	if err != nil {
		return nil, fmt.Errorf("could not parse address as a string: %w", err)
	}

	opts := append(c.opts, client.WithAddress(hostAddr))

	if netAddr.TLSEnabled() {
		opts = append(opts, client.WithTLSConfig(&tls.Config{}))
	}

	cli, err := client.New(opts...)
	if err != nil {
		return nil, err
	}

	c.clients[mAddr] = cli

	return cli, nil
}
