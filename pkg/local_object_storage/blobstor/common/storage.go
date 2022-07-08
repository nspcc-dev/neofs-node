package common

// Storage represents key-value object storage.
// It is used as a building block for a blobstor of a shard.
type Storage interface {
	Get(GetPrm) (GetRes, error)
	GetRange(GetRangePrm) (GetRangeRes, error)
	Exists(ExistsPrm) (ExistsRes, error)
	Put(PutPrm) (PutRes, error)
	Delete(DeletePrm) (DeleteRes, error)
	Iterate(IteratePrm) (IterateRes, error)
}
