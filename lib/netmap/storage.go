package netmap

// GetParams is a group of parameters
// for network map receiving operation.
type GetParams struct {
}

// GetResult is a group of values
// returned by container receiving operation.
type GetResult struct {
	nm *NetMap
}

// Storage is an interface of the storage of NeoFS network map.
type Storage interface {
	GetNetMap(GetParams) (*GetResult, error)
}

// NetMap is a network map getter.
func (s GetResult) NetMap() *NetMap {
	return s.nm
}

// SetNetMap is a network map setter.
func (s *GetResult) SetNetMap(v *NetMap) {
	s.nm = v
}
