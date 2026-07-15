package objectconfig

const (
	// DefaultTombstoneLifetime is the default value of tombstone lifetime in epochs.
	DefaultTombstoneLifetime = 5
)

// Object contains configuration for object service.
type Object struct {
	Delete struct {
		TombstoneLifetime uint64 `mapstructure:"tombstone_lifetime"`
	} `mapstructure:"delete"`
}

// Normalize sets default values for Object configuration.
func (o *Object) Normalize() {
	if o.Delete.TombstoneLifetime <= 0 {
		o.Delete.TombstoneLifetime = DefaultTombstoneLifetime
	}
}
