package wrapper

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
)

// Client represents the Balance contract client.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/morph/client/balance.Client.
type Client = balance.Client

// Wrapper is a wrapper over balance contract
// client which implements:
//  * tool for obtaining the amount of funds in the client's account;
//  * tool for obtaining decimal precision of currency transactions.
//
// Working wrapper must be created via constructor New.
// Using the Wrapper that has been created with new(Wrapper)
// expression (or just declaring a Wrapper variable) is unsafe
// and can lead to panic.
type Wrapper struct {
	client *Client
}

// ErrNilWrapper is returned by functions that expect
// a non-nil Wrapper pointer, but received nil.
var ErrNilWrapper = errors.New("balance contract client wrapper is nil")

// New creates, initializes and returns the Wrapper instance.
//
// If Client is nil, balance.ErrNilClient is returned.
func New(c *Client) (*Wrapper, error) {
	if c == nil {
		return nil, balance.ErrNilClient
	}

	return &Wrapper{
		client: c,
	}, nil
}
