package wrapper

// OwnerID represents the container owner identifier.
// FIXME: correct the definition.
type OwnerID struct{}

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (w *Wrapper) BalanceOf(ownerID OwnerID) (int64, error) {
	panic("implement me")
}
