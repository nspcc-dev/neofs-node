package wrapper

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

// UpdateIRPrm groups parameters of UpdateInnerRing
// invocation.
type UpdateIRPrm struct {
	keys keys.PublicKeys

	client.InvokePrmOptional
}

// SetKeys sets new inner ring keys.
func (u *UpdateIRPrm) SetKeys(keys keys.PublicKeys) {
	u.keys = keys
}

// UpdateInnerRing updates inner ring keys.
func (w *Wrapper) UpdateInnerRing(prm UpdateIRPrm) error {
	args := netmap.UpdateIRPrm{}

	args.SetKeys(prm.keys)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.UpdateInnerRing(args)
}

// GetInnerRingList return current IR list.
func (w *Wrapper) GetInnerRingList() (keys.PublicKeys, error) {
	return w.client.InnerRingList()
}
