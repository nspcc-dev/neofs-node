package boltdb

import (
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
)

type (
	boltBucket struct {
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

const defaultFilePermission = 0777

var errEmptyPath = errors.New("database empty path")

const Name = "boltdb"

func makeCopy(val []byte) []byte {
	tmp := make([]byte, len(val))
	copy(tmp, val)

	return tmp
}

// NewOptions prepares options for badger instance.
func NewOptions(prefix string, v *viper.Viper) (opts Options, err error) {
	prefix = prefix + "." + Name

	opts = Options{
		Options: bbolt.Options{
			// set defaults:
			Timeout:      bbolt.DefaultOptions.Timeout,
			FreelistType: bbolt.DefaultOptions.FreelistType,

			// set config options:
			NoSync:         v.GetBool(prefix + ".no_sync"),
			ReadOnly:       v.GetBool(prefix + ".read_only"),
			NoGrowSync:     v.GetBool(prefix + ".no_grow_sync"),
			NoFreelistSync: v.GetBool(prefix + ".no_freelist_sync"),

			PageSize:        v.GetInt(prefix + ".page_size"),
			MmapFlags:       v.GetInt(prefix + ".mmap_flags"),
			InitialMmapSize: v.GetInt(prefix + ".initial_mmap_size"),
		},

		Name: []byte(Name),
		Perm: defaultFilePermission,
		Path: v.GetString(prefix + ".path"),
	}

	if opts.Path == "" {
		return opts, errEmptyPath
	}

	if tmp := v.GetDuration(prefix + ".lock_timeout"); tmp > 0 {
		opts.Timeout = tmp
	}

	if perm := v.GetUint32(prefix + ".perm"); perm != 0 {
		opts.Perm = os.FileMode(perm)
	}

	base := path.Dir(opts.Path)
	if err := os.MkdirAll(base, opts.Perm); err != nil {
		return opts, errors.Wrapf(err, "could not use `%s` dir", base)
	}

	return opts, nil
}

// NewBucket creates badger-bucket instance.
func NewBucket(opts *Options) (bucket.Bucket, error) {
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

	return &boltBucket{db: db, name: opts.Name}, nil
}
