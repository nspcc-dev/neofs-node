package fsbucket

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
)

// Get value by key.
func (b *Bucket) Get(key []byte) ([]byte, error) {
	p := path.Join(b.dir, stringifyKey(key))
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, bucket.ErrNotFound
	}

	return ioutil.ReadFile(p)
}

// Set value by key.
func (b *Bucket) Set(key, value []byte) error {
	p := path.Join(b.dir, stringifyKey(key))

	return ioutil.WriteFile(p, value, b.perm)
}

// Del value by key.
func (b *Bucket) Del(key []byte) error {
	p := path.Join(b.dir, stringifyKey(key))
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return bucket.ErrNotFound
	}

	return os.Remove(p)
}

// Has checks key exists.
func (b *Bucket) Has(key []byte) bool {
	p := path.Join(b.dir, stringifyKey(key))
	_, err := os.Stat(p)

	return err == nil
}

func listing(root string, fn func(path string, info os.FileInfo) error) error {
	return filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		if fn == nil {
			return nil
		}

		return fn(p, info)
	})
}

// Size of bucket.
func (b *Bucket) Size() (size int64) {
	err := listing(b.dir, func(_ string, info os.FileInfo) error {
		size += info.Size()
		return nil
	})

	if err != nil {
		size = 0
	}

	return
}

// List all bucket items.
func (b *Bucket) List() ([][]byte, error) {
	buckets := make([][]byte, 0)

	err := listing(b.dir, func(p string, info os.FileInfo) error {
		buckets = append(buckets, decodeKey(info.Name()))
		return nil
	})

	return buckets, err
}

// Filter bucket items by closure.
func (b *Bucket) Iterate(handler bucket.FilterHandler) error {
	return listing(b.dir, func(p string, info os.FileInfo) error {
		key := decodeKey(info.Name())
		val, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}

		if !handler(key, val) {
			return bucket.ErrIteratingAborted
		}

		return nil
	})
}

// Close bucket (just empty).
func (b *Bucket) Close() error {
	return os.RemoveAll(b.dir)
}
