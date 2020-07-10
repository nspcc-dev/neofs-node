package boltdb

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
)

type (
	bucket struct {
		db   *bbolt.DB
		name []byte
	}

	// Options groups the BoltDB bucket's options.
	Options struct {
		bbolt.Options
		Name []byte
		Path string
		Perm os.FileMode
	}
)

const (
	defaultFilePermission = 0777

	errEmptyPath = internal.Error("database empty path")
)

var _ core.Bucket = (*bucket)(nil)

func makeCopy(val []byte) []byte {
	tmp := make([]byte, len(val))
	copy(tmp, val)

	return tmp
}

// NewOptions prepares options for badger instance.
func NewOptions(name core.BucketType, v *viper.Viper) (opts Options, err error) {
	key := string(name)
	opts = Options{
		Options: bbolt.Options{
			// set defaults:
			Timeout:      bbolt.DefaultOptions.Timeout,
			FreelistType: bbolt.DefaultOptions.FreelistType,

			// set config options:
			NoSync:         v.GetBool(key + ".no_sync"),
			ReadOnly:       v.GetBool(key + ".read_only"),
			NoGrowSync:     v.GetBool(key + ".no_grow_sync"),
			NoFreelistSync: v.GetBool(key + ".no_freelist_sync"),

			PageSize:        v.GetInt(key + ".page_size"),
			MmapFlags:       v.GetInt(key + ".mmap_flags"),
			InitialMmapSize: v.GetInt(key + ".initial_mmap_size"),
		},

		Name: []byte(name),
		Perm: defaultFilePermission,
		Path: v.GetString(key + ".path"),
	}

	if opts.Path == "" {
		return opts, errEmptyPath
	}

	if tmp := v.GetDuration(key + ".lock_timeout"); tmp > 0 {
		opts.Timeout = tmp
	}

	if perm := v.GetUint32(key + ".perm"); perm != 0 {
		opts.Perm = os.FileMode(perm)
	}

	base := path.Dir(opts.Path)
	if err := os.MkdirAll(base, opts.Perm); err != nil {
		return opts, errors.Wrapf(err, "could not use `%s` dir", base)
	}

	return opts, nil
}

// NewBucket creates badger-bucket instance.
func NewBucket(opts *Options) (core.Bucket, error) {
	log.SetOutput(ioutil.Discard) // disable default logger

	db, err := bbolt.Open(opts.Path, opts.Perm, &opts.Options)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(opts.Name)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &bucket{db: db, name: opts.Name}, nil
}
