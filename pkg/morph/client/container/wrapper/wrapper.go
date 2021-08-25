package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// Wrapper is a wrapper over container contract
// client which implements container storage and
// eACL storage methods.
//
// Working wrapper must be created via constructor New.
// Using the Wrapper that has been created with new(Wrapper)
// expression (or just declaring a Wrapper variable) is unsafe
// and can lead to panic.
type Wrapper struct {
	client *container.Client
}

// Option allows to set an optional
// parameter of ClientWrapper.
type Option func(*opts)

type opts []client.StaticClientOption

func defaultOpts() *opts {
	return new(opts)
}

// Morph returns raw morph client.
func (w Wrapper) Morph() *client.Client {
	return w.client.Morph()
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
		return nil, fmt.Errorf("can't create container static client: %w", err)
	}

	enhancedContainerClient, err := container.New(staticClient)
	if err != nil {
		return nil, fmt.Errorf("can't create container morph client: %w", err)
	}

	return &Wrapper{client: enhancedContainerClient}, nil
}
