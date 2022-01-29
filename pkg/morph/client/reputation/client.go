package reputation

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS reputation contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static reputation contract client
}

const (
	putMethod         = "put"
	getMethod         = "get"
	getByIDMethod     = "getByID"
	listByEpochMethod = "listByEpoch"
)

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, client.ErrNilStaticClient is returned.
func New(c *client.StaticClient) (*Client, error) {
	if c == nil {
		return nil, client.ErrNilStaticClient
	}

	return &Client{client: c}, nil
}

// Morph returns raw morph client.
func (c Client) Morph() *client.Client {
	return c.client.Morph()
}
