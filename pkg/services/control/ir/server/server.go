package control

import (
	"fmt"

	crypto "github.com/nspcc-dev/neofs-crypto"
)

// Server is an entity that serves
// Control service on IR node.
//
// To gain access to the service, any request must be
// signed with a key from the white list.
type Server struct {
	prm Prm

	allowedKeys [][]byte
}

func panicOnPrmValue(n string, v interface{}) {
	const invalidPrmValFmt = "invalid %s parameter (%T): %v"
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Server.
//
// Panics if:
//  - parameterized private key is nil;
//  - parameterized HealthChecker is nil.
//
// Forms white list from all keys specified via
// WithAllowedKeys option and a public key of
// the parameterized private key.
func New(prm Prm, opts ...Option) *Server {
	// verify required parameters
	switch {
	case prm.key == nil:
		panicOnPrmValue("key", prm.key)
	case prm.healthChecker == nil:
		panicOnPrmValue("health checker", prm.healthChecker)
	}

	// compute optional parameters
	o := defaultOptions()

	for _, opt := range opts {
		opt(o)
	}

	return &Server{
		prm: prm,

		allowedKeys: append(o.allowedKeys, crypto.MarshalPublicKey(&prm.key.PublicKey)),
	}
}
