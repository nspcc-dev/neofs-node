package wrapper

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

// Client represents the Netmap contract client.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap.Client.
type Client = netmap.Client

// Wrapper is a wrapper over netmap contract
// client which implements:
//  * network map storage;
//  * tool for peer state updating.
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
var ErrNilWrapper = errors.New("netmap contract client wrapper is nil")

// New creates, initializes and returns the Wrapper instance.
//
// If Client is nil, netmap.ErrNilClient is returned.
func New(c *Client) (*Wrapper, error) {
	if c == nil {
		return nil, netmap.ErrNilClient
	}

	return &Wrapper{
		client: c,
	}, nil
}

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8) (*Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee)
	if err != nil {
		return nil, fmt.Errorf("can't create netmap static client: %w", err)
	}

	enhancedNetmapClient, err := netmap.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create netmap morph client: %w", err)
	}

	return New(enhancedNetmapClient)
}
