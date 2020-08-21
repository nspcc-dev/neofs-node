package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// Client represents the Container contract client.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/morph/client/container.Client.
type Client = container.Client

// CID represents the container identifier.
// FIXME: correct the definition.
type CID struct{}

// Wrapper is a wrapper over container contract
// client which implements container storage and
// eACL storage methods.
//
// Working wrapper must be created via constructor New.
// Using the Wrapper that has been created with new(Wrapper)
// expression (or just declaring a Wrapper variable) is unsafe
// and can lead to panic.
type Wrapper struct {
	client *Client
}

// New creates, initializes and returns the Wrapper instance.
//
// If Client is nil, container.ErrNilClient is returned.
func New(c *Client) (*Wrapper, error) {
	if c == nil {
		return nil, container.ErrNilClient
	}

	return &Wrapper{
		client: c,
	}, nil
}
