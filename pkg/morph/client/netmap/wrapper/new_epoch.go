package wrapper

// Epoch represents the NeoFS epoch.
// FIXME: correct the definition.
type Epoch struct{}

// NewEpoch updates NeoFS epoch number through
// Netmap contract call.
func (w *Wrapper) NewEpoch(e Epoch) error {
	panic("implement me")
}
