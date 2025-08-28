package container

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Container contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Container contract client
}

const (
	putMethod      = "put"
	deleteMethod   = "delete"
	getMethod      = "get"
	getDataMethod  = "getContainerData"
	listMethod     = "containersOf"
	eaclMethod     = "eACL"
	eaclDataMethod = "getEACLData"
	setEACLMethod  = "setEACL"

	putSizeMethod   = "putContainerSize"
	listSizesMethod = "iterateAllContainerSizes"

	addNextEpochNodes         = "addNextEpochNodes"
	commitContainerListUpdate = "commitContainerListUpdate"
	submitObjectPutMethod     = "submitObjectPut"

	// iteratorPrefetchNumber is a number of stack items to prefetch in the
	// first call of iterator-based methods. neo-go's stack elements default
	// limit is 2048, make it less a little.
	iteratorPrefetchNumber = 2000
)

var (
	errNilArgument = errors.New("empty argument")
)

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, opts ...Option) (*Client, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	sc, err := client.NewStatic(cli, contract, o.staticOpts...)
	if err != nil {
		return nil, fmt.Errorf("can't create container static client: %w", err)
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
// parameter of Wrapper.
type Option func(*opts)

type opts struct {
	staticOpts []client.StaticClientOption
}

func defaultOpts() *opts {
	return new(opts)
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

func isMethodNotFoundError(err error, mtd string) bool {
	var exc unwrap.Exception
	if errors.As(err, &exc) {
		return isMethodNotFoundException(string(exc), mtd)
	}
	return isMethodNotFoundException(err.Error(), mtd)
}

func isMethodNotFoundException(msg, mtd string) bool {
	return strings.Contains(msg, "method not found: "+mtd)
}
