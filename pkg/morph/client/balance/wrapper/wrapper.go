package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
)

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
	client *balance.Client
}

// Option allows to set an optional
// parameter of Wrapper.
type Option func(*opts)

type opts []client.StaticClientOption

func defaultOpts() *opts {
	return new(opts)
}

// TryNotary returns option to enable
// notary invocation tries.
func TryNotary() Option {
	return func(o *opts) {
		*o = append(*o, client.TryNotary())
	}
}

// AsAlphabet returns option to sign main TX
// of notary requests with client's private
// key.
//
// Considered to be used by IR nodes only.
func AsAlphabet() Option {
	return func(o *opts) {
		*o = append(*o, client.AsAlphabet())
	}
}

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*Wrapper, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	staticClient, err := client.NewStatic(cli, contract, fee, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of Balance contract: %w", err)
	}

	enhancedBalanceClient, err := balance.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("could not create Balance contract client: %w", err)
	}

	return &Wrapper{client: enhancedBalanceClient}, nil
}
