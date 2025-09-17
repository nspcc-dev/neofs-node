package objectconfig

const (
	// PutPoolSizeDefault is the default value of routine pool size to
	// process object.Put requests in object service.
	PutPoolSizeDefault = 10
	// DefaultTombstoneLifetime is the default value of tombstone lifetime in epochs.
	DefaultTombstoneLifetime = 5
	// DefaultSearchPoolSize is the default value of routine pool size to
	// process object.Search requests in object service.
	DefaultSearchPoolSize = 100
)

// Object contains configuration for object service.
type Object struct {
	Delete struct {
		TombstoneLifetime uint64 `mapstructure:"tombstone_lifetime"`
	} `mapstructure:"delete"`

	Put struct {
		PoolSizeRemote int `mapstructure:"pool_size_remote"`
	} `mapstructure:"put"`

	Search struct {
		PoolSize int `mapstructure:"pool_size"`
	} `mapstructure:"search"`
}

// Normalize sets default values for Object configuration.
func (o *Object) Normalize() {
	if o.Delete.TombstoneLifetime <= 0 {
		o.Delete.TombstoneLifetime = DefaultTombstoneLifetime
	}
	if o.Put.PoolSizeRemote <= 0 {
		o.Put.PoolSizeRemote = PutPoolSizeDefault
	}
	if o.Search.PoolSize <= 0 {
		o.Search.PoolSize = DefaultSearchPoolSize
	}
}
