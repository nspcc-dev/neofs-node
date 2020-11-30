package blobstor

// GetRangeSmallPrm groups the parameters of GetRangeSmall operation.
type GetRangeSmallPrm struct {
	address
	rwRange
	rwBlobovniczaID
}

// GetRangeSmallRes groups resulting values of GetRangeSmall operation.
type GetRangeSmallRes struct {
	rangeData
}

// GetRangeSmall reads data of object payload range from blobovnicza of BLOB storage.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to get payload range
// from any blobovnicza.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
func (b *BlobStor) GetRangeSmall(prm *GetRangeSmallPrm) (*GetRangeSmallRes, error) {
	return b.blobovniczas.getRange(prm)
}
