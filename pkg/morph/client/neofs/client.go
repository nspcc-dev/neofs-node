package neofscontract

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static NeoFS contract client
}

const (
	bindKeysMethod       = "bind"
	unbindKeysMethod     = "unbind"
	alphabetUpdateMethod = "alphabetUpdate"
	chequeMethod         = "cheque"
)

// NewFromMorph wraps client to work with NeoFS contract.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*Client, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}
	*o = append(*o, client.WithStaticFeeIncrement(fee))

	sc, err := client.NewStatic(cli, contract, ([]client.StaticClientOption)(*o)...)
	if err != nil {
		return nil, fmt.Errorf("could not create client of NeoFS contract: %w", err)
	}

	return &Client{client: sc}, nil
}

// ContractAddress returns the address of the associated contract.
func (x *Client) ContractAddress() util.Uint160 {
	return x.client.ContractAddress()
}

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
