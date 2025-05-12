package balance

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Balance contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Balance contract client
}

const (
	transferXMethod = "transferX"
	mintMethod      = "mint"
	burnMethod      = "burn"
	lockMethod      = "lock"
	balanceOfMethod = "balanceOf"
	decimalsMethod  = "decimals"
)

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, opts ...Option) (*Client, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	staticClient, err := client.NewStatic(cli, contract, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of Balance contract: %w", err)
	}

	return &Client{
		client: staticClient,
	}, nil
}

// Option allows to set an optional
// parameter of Wrapper.
type Option func(*opts)

type opts []client.StaticClientOption

func defaultOpts() *opts {
	o := &opts{client.TryNotary()}
	return o
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
