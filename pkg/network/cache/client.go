package cache

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	crypto "github.com/nspcc-dev/neofs-crypto"
)

type (
	// ClientCache is a structure around neofs-api-go/pkg/client to reuse
	// already created clients.
	ClientCache struct {
		mu      *sync.RWMutex
		clients map[string]*client.Client
	}
)

// NewSDKClientCache creates instance of client cache.
func NewSDKClientCache() *ClientCache {
	return &ClientCache{
		mu:      new(sync.RWMutex),
		clients: make(map[string]*client.Client),
	}
}

// Get function returns existing client or creates a new one. Consider passing
// connection options to specify details for client, but don't forget that two
// different set of options should provide two different clients.
func (c *ClientCache) Get(key *ecdsa.PrivateKey, address string) (*client.Client, error) {
	id := uniqueID(key, address)

	c.mu.RLock()
	if cli, ok := c.clients[id]; ok {
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
	if cli, ok := c.clients[id]; ok {
		return cli, nil
	}

	cli, err := client.New(key, client.WithAddress(address))
	if err != nil {
		return nil, err
	}

	c.clients[id] = cli

	return cli, nil
}

func uniqueID(key *ecdsa.PrivateKey, address string) string {
	keyFingerprint := sha256.Sum256(crypto.MarshalPrivateKey(key))

	return hex.EncodeToString(keyFingerprint[:]) + address
}
