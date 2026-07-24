package common

import (
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
)

// RequestTokens groups request tokens.
type RequestTokens struct {
	Session   *sessionv2.Token
	SessionV1 *session.Object
	Bearer    *bearer.Token

	// AuthenticatedPeerPublicKey is the compressed ECDSA public key of an
	// inter-node TLS peer authenticated by the transport layer.
	AuthenticatedPeerPublicKey []byte
}
