package structure

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Verify calls network.VerifyAddress.
func (v *Validator) Verify(n netmap.NodeInfo) error {
	err := network.VerifyMultiAddress(n)
	if err != nil {
		return fmt.Errorf("could not verify multiaddress: %w", err)
	}

	attrM := make(map[string]struct{}, n.NumberOfAttributes())
	for key := range n.Attributes() {
		if _, alreadyHave := attrM[key]; alreadyHave {
			return fmt.Errorf("repeating node attribute: '%s'", key)
		}
		attrM[key] = struct{}{}
	}

	return nil
}
