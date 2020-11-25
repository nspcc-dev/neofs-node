package blobstor

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	address
}

// ExistsRes groups resulting values of Exists operation.
type ExistsRes struct {
	exists bool
}

// Exists returns the fact that the object is in BLOB storage.
func (r ExistsRes) Exists() bool {
	return r.exists
}

// Exists checks if object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(prm *ExistsPrm) (*ExistsRes, error) {
	panic("implement me")
}
