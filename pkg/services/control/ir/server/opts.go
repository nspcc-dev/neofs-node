package control

// Option specifies Server's optional parameter.
type Option func(*options)

type options struct {
	allowedKeys [][]byte
}

func defaultOptions() *options {
	return new(options)
}

// WithAllowedKeys returns option to add public keys
// to white list of the Control service.
func WithAllowedKeys(keys [][]byte) Option {
	return func(o *options) {
		o.allowedKeys = append(o.allowedKeys, keys...)
	}
}
