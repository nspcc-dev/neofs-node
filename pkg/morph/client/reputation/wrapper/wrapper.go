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

// Option allows to set an optional
// parameter of ClientWrapper.
type Option func(*opts)

type opts []client.StaticClientOption

func defaultOpts() *opts {
	return new(opts)
}

// TryNotaryInvoke returns option to enable
// notary invocation tries.
func TryNotary() Option {
	return func(o *opts) {
		*o = append(*o, client.TryNotary())
	}
}

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*ClientWrapper, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	staticClient, err := client.NewStatic(cli, contract, fee, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of reputation contract: %w", err)
	}

	enhancedRepurationClient, err := reputation.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("could not create reputation contract client: %w", err)
	}

	return (*ClientWrapper)(enhancedRepurationClient), nil
}
