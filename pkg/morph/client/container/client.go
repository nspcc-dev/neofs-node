package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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
	putMethod     = "put"
	deleteMethod  = "delete"
	getMethod     = "get"
	listMethod    = "list"
	eaclMethod    = "eACL"
	setEACLMethod = "setEACL"

	startEstimationMethod = "startContainerEstimation"
	stopEstimationMethod  = "stopContainerEstimation"

	putSizeMethod   = "putContainerSize"
	listSizesMethod = "iterateAllContainerSizes"

	// putNamedMethod is method name for container put with an alias. It is exported to provide custom fee.
	putNamedMethod = "putNamed"

	addNextEpochNodes         = "addNextEpochNodes"
	commitContainerListUpdate = "commitContainerListUpdate"
	submitObjectPutMethod     = "submitObjectPut"
)

var (
	errNilArgument = errors.New("empty argument")
)

// NewFromMorph returns the wrapper instance from the raw morph client.
//
// Specified fee is used for all operations by default. If WithCustomFeeForNamedPut is provided,
// the customized fee is used for Put operations with named containers.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...Option) (*Client, error) {
	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	if o.feePutNamedSet {
		o.staticOpts = append(o.staticOpts, client.WithCustomFee(putNamedMethod, o.feePutNamed))
	}

	sc, err := client.NewStatic(cli, contract, fee, o.staticOpts...)
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
	feePutNamedSet bool
	feePutNamed    fixedn.Fixed8

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

// WithCustomFeeForNamedPut returns option to specify custom fee for each Put operation with named container.
func WithCustomFeeForNamedPut(fee fixedn.Fixed8) Option {
	return func(o *opts) {
		o.feePutNamed = fee
		o.feePutNamedSet = true
	}
}

func decodeSignature(bPubKey, sig []byte) (neofscrypto.Signature, error) {
	var pubKey neofsecdsa.PublicKeyRFC6979

	err := pubKey.Decode(bPubKey)
	if err != nil {
		return neofscrypto.Signature{}, fmt.Errorf("decode public key: %w", err)
	}

	return neofscrypto.NewSignature(neofscrypto.ECDSA_DETERMINISTIC_SHA256, &pubKey, sig), nil
}
