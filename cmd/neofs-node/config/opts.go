package config

type opts struct {
	path string
}

func defaultOpts() *opts {
	return new(opts)
}

// Option allows to set optional parameter of the Config.
type Option func(*opts)

// WithConfigFile returns option to set system path
// to the configuration file.
func WithConfigFile(path string) Option {
	return func(o *opts) {
		o.path = path
	}
}
