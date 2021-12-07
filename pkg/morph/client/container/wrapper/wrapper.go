package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/internal"
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
	internal.StaticClient

	client *container.Client
}

// Option allows to set an optional
// parameter of Wrapper.
type Option func(*opts)

type opts struct {
	feePutNamedSet bool
	feePutNamed    fixedn.Fixed8

	staticOpts []client.StaticClientOption
}

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
		o.staticOpts = append(o.staticOpts, client.TryNotary())
	}
}

// AsAlphabet returns option to sign main TX
// of notary requests with client's private
// key.
//
// Considered to be used by IR nodes only.
func AsAlphabet() Option {
	return func(o *opts) {
		o.staticOpts = append(o.staticOpts, client.AsAlphabet())
	}
}

// WithCustomFeeForNamedPut returns option to specify custom fee for each Put operation with named container.
func WithCustomFeeForNamedPut(fee fixedn.Fixed8) Option {
	return func(o *opts) {
		o.feePutNamed = fee
		o.feePutNamedSet = true
	}
}

// NewFromMorph returns the wrapper instance from the raw morph client.
//
// Specified fee is used for all operations by default. If WithCustomFeeForNamedPut is provided,
// the customized fee is used for Put operations with named containers.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*Wrapper, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	// below is working but not the best solution to customize fee for PutNamed operation
	// It is done like that because container package doesn't provide option to specify the fee.
	// In the future, we will possibly get rid of the container package at all.
	const methodNamePutNamed = "putNamed"

	var (
		staticOpts = o.staticOpts
		cnrOpts    = make([]container.Option, 0, 1)
	)

	if o.feePutNamedSet {
		staticOpts = append(staticOpts, client.WithCustomFee(methodNamePutNamed, o.feePutNamed))
		cnrOpts = append(cnrOpts, container.WithPutNamedMethod(methodNamePutNamed))
	}

	staticClient, err := client.NewStatic(cli, contract, fee, staticOpts...)
	if err != nil {
		return nil, fmt.Errorf("can't create container static client: %w", err)
	}

	enhancedContainerClient, err := container.New(staticClient, cnrOpts...)
	if err != nil {
		return nil, fmt.Errorf("can't create container morph client: %w", err)
	}

	return &Wrapper{
		StaticClient: staticClient,
		client:       enhancedContainerClient,
	}, nil
}
