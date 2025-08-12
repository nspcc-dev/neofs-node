package putsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
)

// RelayFunc relays request using given connection to SN.
type RelayFunc = func(client.NodeInfo, client.MultiAddressClient) error

type PutInitOptions struct {
	CopiesNumber uint32

	Relay RelayFunc
}
