package maddress

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
)

// VerifyAndUpdate calls network.VerifyAddress.
func (v *Validator) VerifyAndUpdate(n *netmap.NodeInfo) error {
	err := network.VerifyMultiAddress(n)
	if err != nil {
		return fmt.Errorf("could not verify multiaddress: %w", err)
	}

	return nil
}
