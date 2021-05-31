package wrapper

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// SetInnerRing updates inner ring keys.
func (w *Wrapper) SetInnerRing(keys keys.PublicKeys) error {
	return w.client.SetInnerRing(keys)
}
