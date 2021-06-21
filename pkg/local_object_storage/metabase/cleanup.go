package meta

// CleanUpPrm groups the parameters of CleanUp operation.
type CleanUpPrm struct{}

// CleanUpRes groups resulting values of CleanUp operation.
type CleanUpRes struct{}

// CleanUp removes empty buckets from metabase.
func (db *DB) CleanUp(prm *CleanUpPrm) (res *CleanUpRes, err error) {
	return
}
