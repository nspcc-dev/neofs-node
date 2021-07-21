package wrapper

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// UpdateInnerRing updates inner ring keys.
func (w *Wrapper) UpdateInnerRing(keys keys.PublicKeys) error {
	return w.client.UpdateInnerRing(keys)
}

// GetInnerRingList return current IR list.
func (w *Wrapper) GetInnerRingList() (keys.PublicKeys, error) {
	return w.client.InnerRingList()
}
