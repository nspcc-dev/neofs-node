package blobovniczatree

// GetRangeSmallPrm groups the parameters of GetRangeSmall operation.
type GetRangeSmallPrm struct {
	address
	rwRange
	rwBlobovniczaID
}

// GetRangeSmallRes groups the resulting values of GetRangeSmall operation.
type GetRangeSmallRes struct {
	rangeData
}
