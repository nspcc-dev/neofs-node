package wrapper

// NetMap represents the NeoFS network map.
// FIXME: correct the definition.
type NetMap struct{}

// GetNetMap receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it.
func (w *Wrapper) GetNetMap() (*NetMap, error) {
	panic("implement me")
}
