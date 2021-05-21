package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

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
	client *netmap.Client
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

	return &Wrapper{client: enhancedNetmapClient}, nil
}
