package container

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Put structure of container.Put notification from morph chain.
type Put struct {
	rawContainer []byte
	signature    []byte
	publicKey    *keys.PublicKey
}

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Container is a marshalled container structure, defined in API.
func (p Put) Container() []byte { return p.rawContainer }

// Signature of marshalled container by container owner.
func (p Put) Signature() []byte { return p.signature }

// PublicKey of container owner.
func (p Put) PublicKey() *keys.PublicKey { return p.publicKey }

// ParsePut from notification into container event structure.
func ParsePut(params []smartcontract.Parameter) (event.Event, error) {
	var (
		ev  Put
		err error
	)

	if ln := len(params); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse container
	ev.rawContainer, err = client.BytesFromStackParameter(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get container")
	}

	// parse signature
	ev.signature, err = client.BytesFromStackParameter(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get signature")
	}

	// parse public key
	key, err := client.BytesFromStackParameter(params[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get public key")
	}

	ev.publicKey, err = keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return nil, errors.Wrap(err, "could not parse public key")
	}

	return ev, nil
}
