package shard

// WeightValues groups values of Shard weight parameters.
type WeightValues struct {
	// Amount of free disk space. Measured in kilobytes.
	FreeSpace uint64
}

// WeightValues returns current weight values of the Shard.
func (s *Shard) WeightValues() WeightValues {
	return s.info.WeightValues
}
