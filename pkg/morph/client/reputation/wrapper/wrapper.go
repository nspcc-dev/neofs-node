package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
)

// ClientWrapper is a wrapper over reputation contract
// client which implements storage of reputation values.
type ClientWrapper reputation.Client

// WrapClient wraps reputation contract client and returns ClientWrapper instance.
func WrapClient(c *reputation.Client) *ClientWrapper {
	return (*ClientWrapper)(c)
}

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8) (*ClientWrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of reputation contract: %w", err)
	}

	enhancedRepurationClient, err := reputation.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("could not create reputation contract client: %w", err)
	}

	return WrapClient(enhancedRepurationClient), nil
}
