package config

type opts struct {
	path     string
	validate bool
}

func defaultOpts() *opts {
	return &opts{
		validate: true,
	}
}

// Option allows to set an optional parameter of the Config.
type Option func(*opts)

// WithConfigFile returns an option to set the system path
// to the configuration file.
func WithConfigFile(path string) Option {
	return func(o *opts) {
		o.path = path
	}
}

// WithValidate returns an option that is responsible
// for whether the config structure needs to be validated.
func WithValidate(validate bool) Option {
	return func(o *opts) {
		o.validate = validate
	}
}
