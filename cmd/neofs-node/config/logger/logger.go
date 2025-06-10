package loggerconfig

const (
	// LevelDefault is the default logger level.
	LevelDefault = "info"
	// EncodingDefault is the default logger encoding.
	EncodingDefault = "console"
)

// Sampling configures log sampling.
type Sampling struct {
	Enabled bool `mapstructure:"enabled"`
}

// Logger contains configuration for logger.
type Logger struct {
	Level     string   `mapstructure:"level"`
	Encoding  string   `mapstructure:"encoding"`
	Timestamp bool     `mapstructure:"timestamp"`
	Sampling  Sampling `mapstructure:"sampling"`
}

// Normalize sets default values for Logger configuration.
func (l *Logger) Normalize() {
	if l.Level == "" {
		l.Level = LevelDefault
	}
	if l.Encoding == "" {
		l.Encoding = EncodingDefault
	}
}
