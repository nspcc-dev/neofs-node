package pilorama

// Info groups the information about the pilorama.
type Info struct {
	// Path contains path to the root-directory of the pilorama.
	Path string
	// Backend is the pilorama storage type. Either "boltdb" or "memory".
	Backend string
}

// DumpInfo implements the ForestStorage interface.
func (t *boltForest) DumpInfo() Info {
	return Info{
		Path:    t.path,
		Backend: "boltdb",
	}
}

// DumpInfo implements the ForestStorage interface.
func (f *memoryForest) DumpInfo() Info {
	return Info{
		Backend: "memory",
	}
}
