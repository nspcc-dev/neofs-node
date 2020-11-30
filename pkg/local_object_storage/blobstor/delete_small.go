package blobstor

// DeleteSmallPrm groups the parameters of DeleteSmall operation.
type DeleteSmallPrm struct {
	address
	rwBlobovniczaID
}

// DeleteSmallRes groups resulting values of DeleteSmall operation.
type DeleteSmallRes struct{}

// DeleteSmall removes object from blobovnicza of BLOB storage.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to
// find and remove object from any blobovnicza.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns ErrObjectNotFound if there is no object to delete.
func (b *BlobStor) DeleteSmall(prm *DeleteSmallPrm) (*DeleteSmallRes, error) {
	panic("implement me")
}
