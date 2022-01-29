package neofsid

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS ID contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static NeoFS ID contract client
}

const (
	keyListingMethod = "key"
	addKeysMethod    = "addKey"
	removeKeysMethod = "removeKey"
)

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, panic occurs.
func New(c *client.StaticClient) *Client {
	if c == nil {
		panic("static client is nil")
	}

	return &Client{client: c}
}
