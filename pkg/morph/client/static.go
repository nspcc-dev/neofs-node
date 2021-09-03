package client

import (
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// StaticClient is a wrapper over Neo:Morph client
// that invokes single smart contract methods with fixed fee.
//
// Working static client must be created via constructor NewStatic.
// Using the StaticClient that has been created with new(StaticClient)
// expression (or just declaring a StaticClient variable) is unsafe
// and can lead to panic.
type StaticClient struct {
	tryNotary bool

	client *Client // neo-go client instance

	scScriptHash util.Uint160 // contract script-hash

	fee fixedn.Fixed8 // invocation fee
}

type staticOpts struct {
	tryNotary bool
}

// StaticClientOption allows to set an optional
// parameter of StaticClient.
type StaticClientOption func(*staticOpts)

func defaultStaticOpts() *staticOpts {
	return new(staticOpts)
}

// TryNotary returns option to enable
// notary invocation tries.
func TryNotary() StaticClientOption {
	return func(o *staticOpts) {
		o.tryNotary = true
	}
}

// ErrNilStaticClient is returned by functions that expect
// a non-nil StaticClient pointer, but received nil.
var ErrNilStaticClient = errors.New("static client is nil")

// NewStatic creates, initializes and returns the StaticClient instance.
//
// If provided Client instance is nil, ErrNilClient is returned.
func NewStatic(client *Client, scriptHash util.Uint160, fee fixedn.Fixed8, opts ...StaticClientOption) (*StaticClient, error) {
	if client == nil {
		return nil, ErrNilClient
	}

	o := defaultStaticOpts()

	for i := range opts {
		opts[i](o)
	}

	return &StaticClient{
		tryNotary:    o.tryNotary,
		client:       client,
		scScriptHash: scriptHash,
		fee:          fee,
	}, nil
}

// Morph return wrapped raw morph client.
func (s StaticClient) Morph() *Client {
	return s.client
}

// Invoke calls Invoke method of Client with static internal script hash and fee.
// Supported args types are the same as in Client.
//
// If TryNotary is provided, calls NotaryInvoke on Client.
func (s StaticClient) Invoke(method string, args ...interface{}) error {
	if s.tryNotary {
		return s.client.NotaryInvoke(s.scScriptHash, s.fee, method, args...)
	}

	return s.client.Invoke(
		s.scScriptHash,
		s.fee,
		method,
		args...,
	)
}

// TestInvoke calls TestInvoke method of Client with static internal script hash.
func (s StaticClient) TestInvoke(method string, args ...interface{}) ([]stackitem.Item, error) {
	return s.client.TestInvoke(
		s.scScriptHash,
		method,
		args...,
	)
}
