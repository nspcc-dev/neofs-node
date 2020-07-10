package fsbucket

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/lib/core"
)

// Get value by key.
func (b *bucket) Get(key []byte) ([]byte, error) {
	p := path.Join(b.dir, stringifyKey(key))
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, core.ErrNotFound
	}

	return ioutil.ReadFile(p)
}

// Set value by key.
func (b *bucket) Set(key, value []byte) error {
	p := path.Join(b.dir, stringifyKey(key))

	return ioutil.WriteFile(p, value, b.perm)
}

// Del value by key.
func (b *bucket) Del(key []byte) error {
	p := path.Join(b.dir, stringifyKey(key))
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return core.ErrNotFound
	}

	return os.Remove(p)
}

// Has checks key exists.
func (b *bucket) Has(key []byte) bool {
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
func (b *bucket) Size() (size int64) {
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
func (b *bucket) List() ([][]byte, error) {
	buckets := make([][]byte, 0)

	err := listing(b.dir, func(p string, info os.FileInfo) error {
		buckets = append(buckets, decodeKey(info.Name()))
		return nil
	})

	return buckets, err
}

// Filter bucket items by closure.
func (b *bucket) Iterate(handler core.FilterHandler) error {
	return listing(b.dir, func(p string, info os.FileInfo) error {
		key := decodeKey(info.Name())
		val, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}

		if !handler(key, val) {
			return core.ErrIteratingAborted
		}

		return nil
	})
}

// Close bucket (just empty).
func (b *bucket) Close() error {
	return os.RemoveAll(b.dir)
}
