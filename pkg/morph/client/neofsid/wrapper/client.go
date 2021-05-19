package neofsid

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
)

// ClientWrapper is a wrapper over NeoFS ID contract
// client which provides convenient methods for
// working with a contract.
//
// Working ClientWrapper must be created via Wrap.
type ClientWrapper neofsid.Client

// Wrap creates, initializes and returns the ClientWrapper instance.
//
// If c is nil, panic occurs.
func Wrap(c *neofsid.Client) *ClientWrapper {
	if c == nil {
		panic("neofs ID client is nil")
	}

	return (*ClientWrapper)(c)
}
