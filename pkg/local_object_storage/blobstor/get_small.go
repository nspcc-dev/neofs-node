package blobstor

// GetSmallPrm groups the parameters of GetSmallPrm operation.
type GetSmallPrm struct {
	address
	rwBlobovniczaID
}

// GetSmallRes groups resulting values of GetSmall operation.
type GetSmallRes struct {
	roObject
}

// GetSmall reads the object from blobovnicza of BLOB storage by address.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to get object
// from any blobovnicza.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns apistatus.ObjectNotFound if requested object is missing in blobovnicza(s).
func (b *BlobStor) GetSmall(prm *GetSmallPrm) (*GetSmallRes, error) {
	return b.blobovniczas.get(prm)
}
