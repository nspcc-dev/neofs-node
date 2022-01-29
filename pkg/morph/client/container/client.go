package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Container contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Container contract client
}

const (
	putMethod     = "put"
	deleteMethod  = "delete"
	getMethod     = "get"
	listMethod    = "list"
	eaclMethod    = "eACL"
	setEACLMethod = "setEACL"

	startEstimationMethod = "startContainerEstimation"
	stopEstimationMethod  = "stopContainerEstimation"

	putSizeMethod   = "putContainerSize"
	listSizesMethod = "listContainerSizes"
	getSizeMethod   = "getContainerSize"

	// PutNamedMethod is method name for container put with an alias. It is exported to provide custom fee.
	PutNamedMethod = "putNamed"
)

// New creates, initializes and returns the Client instance.
func New(c *client.StaticClient) *Client {
	return &Client{client: c}
}

// Morph returns raw morph client.
func (c Client) Morph() *client.Client {
	return c.client.Morph()
}
