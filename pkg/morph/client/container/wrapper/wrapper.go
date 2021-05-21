package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// Client represents the Container contract client.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/morph/client/container.Client.
type Client = container.Client

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

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8) (*Wrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee)
	if err != nil {
		return nil, fmt.Errorf("can't create container static client: %w", err)
	}

	enhancedContainerClient, err := container.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create container morph client: %w", err)
	}

	return &Wrapper{client: enhancedContainerClient}, nil
}
