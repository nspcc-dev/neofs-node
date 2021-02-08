package continentsdb

// Option sets an optional parameter of DB.
type Option func(*options)

type options struct{}

func defaultOpts() *options {
	return &options{}
}
