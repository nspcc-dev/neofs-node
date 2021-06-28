package csvlocode

import (
	"io/fs"
)

// Option sets an optional parameter of Table.
type Option func(*options)

type options struct {
	mode fs.FileMode

	extraPaths []string
}

func defaultOpts() *options {
	return &options{
		mode: 0700,
	}
}

// WithExtraPaths returns option to add extra paths
// to UN/LOCODE tables in csv format.
func WithExtraPaths(ps ...string) Option {
	return func(o *options) {
		o.extraPaths = append(o.extraPaths, ps...)
	}
}
