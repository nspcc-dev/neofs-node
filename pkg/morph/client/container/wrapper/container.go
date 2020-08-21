package wrapper

// OwnerID represents the container owner identifier.
// FIXME: correct the definition.
type OwnerID struct{}

// Container represents the NeoFS Container structure.
// FIXME: correct the definition.
type Container struct{}

// Put saves passed container structure in NeoFS system
// through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (w *Wrapper) Put(cnr *Container) (*CID, error) {
	panic("implement me")
}

// Get reads the container from NeoFS system by identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (w *Wrapper) Get(cid CID) (*Container, error) {
	panic("implement me")
}

// Delete removes the container from NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused
// the removal to interrupt.
func (w *Wrapper) Delete(cid CID) error {
	panic("implement me")
}

// List returns a list of container identifiers belonging
// to the specified owner of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to owner identifier is nil.
func (w *Wrapper) List(ownerID *OwnerID) ([]CID, error) {
	panic("implement me")
}
