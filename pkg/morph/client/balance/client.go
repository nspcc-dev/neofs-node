package balance

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Balance contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Balance contract client
}

const (
	transferXMethod = "transferX"
	mintMethod      = "mint"
	burnMethod      = "burn"
	lockMethod      = "lock"
	balanceOfMethod = "balanceOf"
	decimalsMethod  = "decimals"
)

// New creates, initializes and returns the Client instance.
func New(c *client.StaticClient) *Client {
	return &Client{client: c}
}
