package locodebolt

import (
	"io/fs"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// Option sets an optional parameter of DB.
type Option func(*options)

type options struct {
	mode fs.FileMode

	boltOpts *bbolt.Options
}

func defaultOpts() *options {
	return &options{
		mode: os.ModePerm, // 0777
		boltOpts: &bbolt.Options{
			Timeout: 3 * time.Second,
		},
	}
}

// ReadOnly enables read-only mode of the DB.
//
// Do not call DB.Put method on instances with
// this option: the behavior is undefined.
func ReadOnly() Option {
	return func(o *options) {
		o.boltOpts.ReadOnly = true
	}
}
