package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Put structure of container.Put notification from morph chain.
type Put struct {
	rawContainer []byte
	signature    []byte
	publicKey    []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Container is a marshalled container structure, defined in API.
func (p Put) Container() []byte { return p.rawContainer }

// Signature of marshalled container by container owner.
func (p Put) Signature() []byte { return p.signature }

// PublicKey of container owner.
func (p Put) PublicKey() []byte { return p.publicKey }

// ParsePut from notification into container event structure.
func ParsePut(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Put
		err error
	)

	if ln := len(params); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse container
	ev.rawContainer, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get container: %w", err)
	}

	// parse signature
	ev.signature, err = client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get signature: %w", err)
	}

	// parse public key
	ev.publicKey, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get public key: %w", err)
	}

	return ev, nil
}
