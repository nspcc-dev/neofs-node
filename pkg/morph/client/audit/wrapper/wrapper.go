package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
)

// ClientWrapper is a wrapper over Audit contract
// client which implements storage of audit results.
type ClientWrapper audit.Client

// WrapClient wraps Audit contract client and returns ClientWrapper instance.
func WrapClient(c *audit.Client) *ClientWrapper {
	return (*ClientWrapper)(c)
}
