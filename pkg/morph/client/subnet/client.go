package morphsubnet

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client represents Subnet contract client.
//
// Client should be preliminary initialized (see Init method).
type Client struct {
	client *client.StaticClient
}

// InitPrm groups parameters of Client's initialization.
type InitPrm struct {
	base *client.Client

	addr util.Uint160

	modeSet bool
	mode    Mode
}

// SetBaseClient sets basic morph client.
func (x *InitPrm) SetBaseClient(base *client.Client) {
	x.base = base
}

// SetContractAddress sets address of Subnet contract in NeoFS sidechain.
func (x *InitPrm) SetContractAddress(addr util.Uint160) {
	x.addr = addr
}

// Mode regulates client work mode.
type Mode uint8

const (
	_ Mode = iota

	// NonNotary makes client to work in non-notary environment.
	NonNotary

	// NotaryAlphabet makes client to use its internal key for signing the notary requests.
	NotaryAlphabet

	// NotaryNonAlphabet makes client to not use its internal key for signing the notary requests.
	NotaryNonAlphabet

	lastMode
)

// SetMode makes client to work with non-notary sidechain.
// By default, NonNotary is used.
func (x *InitPrm) SetMode(mode Mode) {
	x.modeSet = true
	x.mode = mode
}

// Init initializes client with specified parameters.
//
// Base client must be set.
func (x *Client) Init(prm InitPrm) error {
	if prm.base == nil {
		panic("missing base morph client")
	}

	if !prm.modeSet {
		prm.mode = NonNotary
	}

	var opts []client.StaticClientOption

	switch prm.mode {
	default:
		panic(fmt.Sprintf("invalid work mode %d", prm.mode))
	case NonNotary:
	case NotaryNonAlphabet:
		opts = []client.StaticClientOption{client.TryNotary()}
	case NotaryAlphabet:
		opts = []client.StaticClientOption{client.TryNotary(), client.AsAlphabet()}
	}

	var err error

	x.client, err = client.NewStatic(prm.base, prm.addr, 0, opts...)

	return err
}
