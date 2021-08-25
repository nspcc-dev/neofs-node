package neofscontract

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
)

// ClientWrapper is a wrapper over NeoFS contract
// client which provides convenient methods for
// working with a contract.
//
// Working ClientWrapper must be created via NewFromMorph.
type ClientWrapper neofscontract.Client

// Option allows to set an optional
// parameter of ClientWrapper.
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

// NewFromMorph wraps client to work with NeoFS contract.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*ClientWrapper, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	sc, err := client.NewStatic(cli, contract, fee, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create client of NeoFS contract: %w", err)
	}

	return (*ClientWrapper)(neofscontract.New(sc)), nil
}
