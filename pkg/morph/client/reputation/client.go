package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS reputation contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static reputation contract client
}

const (
	putMethod         = "put"
	getMethod         = "get"
	getByIDMethod     = "getByID"
	listByEpochMethod = "listByEpoch"
)

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, opts ...Option) (*Client, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	sc, err := client.NewStatic(cli, contract, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of reputation contract: %w", err)
	}

	return &Client{client: sc}, nil
}

// Morph returns raw morph client.
func (c Client) Morph() *client.Client {
	return c.client.Morph()
}

// ContractAddress returns the address of the associated contract.
func (c Client) ContractAddress() util.Uint160 {
	return c.client.ContractAddress()
}

// Option allows to set an optional
// parameter of ClientWrapper.
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
