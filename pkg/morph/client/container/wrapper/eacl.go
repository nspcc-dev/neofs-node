package wrapper

// Table represents extended ACL rule table.
// FIXME: correct the definition.
type Table struct{}

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (w *Wrapper) GetEACL(cid CID) (Table, error) {
	panic("implement me")
}

// PutEACL saves the extended ACL table in NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) PutEACL(cid CID, table Table, sig []byte) error {
	panic("implement me")
}
