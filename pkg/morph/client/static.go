package client

import (
	"errors"
	"fmt"

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
	alpha     bool // use client's key to sign notary request's main TX

	client *Client // neo-go client instance

	scScriptHash util.Uint160 // contract script-hash

	fee fixedn.Fixed8 // invocation fee
}

type staticOpts struct {
	tryNotary bool
	alpha     bool
}

// StaticClientOption allows to set an optional
// parameter of StaticClient.
type StaticClientOption func(*staticOpts)

func defaultStaticOpts() *staticOpts {
	return new(staticOpts)
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
		alpha:        o.alpha,
		client:       client,
		scScriptHash: scriptHash,
		fee:          fee,
	}, nil
}

// Morph return wrapped raw morph client.
func (s StaticClient) Morph() *Client {
	return s.client
}

// InvokePrm groups parameters of the Invoke operation.
type InvokePrm struct {
	// required parameters
	method string
	args   []interface{}

	// optional parameters
	InvokePrmOptional
}

// InvokePrmOptional groups optional parameters of the Invoke operation.
type InvokePrmOptional struct {
	// hash is an optional hash of the transaction
	// that generated the notification that required
	// to invoke notary request.
	// It is used to generate same but unique nonce and
	// `validUntilBlock` values by all notification
	// receivers.
	hash *util.Uint256
}

// SetMethod sets method of the contract to call.
func (i *InvokePrm) SetMethod(method string) {
	i.method = method
}

// SetArgs sets arguments of the contact call.
func (i *InvokePrm) SetArgs(args ...interface{}) {
	i.args = args
}

// SetHash sets optional hash of the transaction.
// If hash is set and notary is enabled, StaticClient
// uses it for notary nonce and `validUntilBlock`
// calculation.
func (i *InvokePrmOptional) SetHash(hash util.Uint256) {
	i.hash = &hash
}

// Invoke calls Invoke method of Client with static internal script hash and fee.
// Supported args types are the same as in Client.
//
// If TryNotary is provided:
//     - if AsAlphabet is provided, calls NotaryInvoke;
//     - otherwise, calls NotaryInvokeNotAlpha.
func (s StaticClient) Invoke(prm InvokePrm) error {
	if s.tryNotary {
		if s.alpha {
			var (
				nonce uint32 = 1
				vubP  *uint32
				vub   uint32
				err   error
			)

			if prm.hash != nil {
				nonce, vub, err = s.client.CalculateNonceAndVUB(*prm.hash)
				if err != nil {
					return fmt.Errorf("could not calculate nonce and VUB for notary alphabet invoke: %w", err)
				}

				vubP = &vub
			}

			return s.client.NotaryInvoke(s.scScriptHash, s.fee, nonce, vubP, prm.method, prm.args...)
		}

		return s.client.NotaryInvokeNotAlpha(s.scScriptHash, s.fee, prm.method, prm.args...)
	}

	return s.client.Invoke(
		s.scScriptHash,
		s.fee,
		prm.method,
		prm.args...,
	)
}

// TestInvokePrm groups parameters of the TestInvoke operation.
type TestInvokePrm struct {
	method string
	args   []interface{}
}

// SetMethod sets method of the contract to call.
func (ti *TestInvokePrm) SetMethod(method string) {
	ti.method = method
}

// SetArgs sets arguments of the contact call.
func (ti *TestInvokePrm) SetArgs(args ...interface{}) {
	ti.args = args
}

// TestInvoke calls TestInvoke method of Client with static internal script hash.
func (s StaticClient) TestInvoke(prm TestInvokePrm) ([]stackitem.Item, error) {
	return s.client.TestInvoke(
		s.scScriptHash,
		prm.method,
		prm.args...,
	)
}

// ContractAddress returns the address of the associated contract.
func (s StaticClient) ContractAddress() util.Uint160 {
	return s.scScriptHash
}

// TryNotary returns option to enable
// notary invocation tries.
func TryNotary() StaticClientOption {
	return func(o *staticOpts) {
		o.tryNotary = true
	}
}

// AsAlphabet returns option to sign main TX
// of notary requests with client's private
// key.
//
// Considered to be used by IR nodes only.
func AsAlphabet() StaticClientOption {
	return func(o *staticOpts) {
		o.alpha = true
	}
}
