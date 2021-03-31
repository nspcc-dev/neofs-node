package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
)

// ClientWrapper is a wrapper over reputation contract
// client which implements storage of reputation values.
type ClientWrapper reputation.Client

// WrapClient wraps reputation contract client and returns ClientWrapper instance.
func WrapClient(c *reputation.Client) *ClientWrapper {
	return (*ClientWrapper)(c)
}
