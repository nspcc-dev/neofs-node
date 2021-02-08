package locodebolt

import (
	"os"

	"go.etcd.io/bbolt"
)

// Option sets an optional parameter of DB.
type Option func(*options)

type options struct {
	mode os.FileMode

	boltOpts *bbolt.Options
}

func defaultOpts() *options {
	return &options{
		mode: os.ModePerm, // 0777
	}
}
