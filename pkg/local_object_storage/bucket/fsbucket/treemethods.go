package fsbucket

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
)

const queueCap = 1000

func stringifyHexKey(key []byte) string {
	return hex.EncodeToString(key)
}

func decodeHexKey(key string) ([]byte, error) {
	k, err := hex.DecodeString(key)
	if err != nil {
		return nil, err
	}

	return k, nil
}

// treePath returns slice of the dir names that contain the path
// and filename, e.g. 0xabcdef => []string{"ab", "cd"}, "abcdef".
// In case of errors - return nil slice.
func (b *treeBucket) treePath(key []byte) ([]string, string) {
	filename := stringifyHexKey(key)
	if len(filename) <= b.prefixLength*b.depth {
		return nil, filename
	}

	filepath := filename
	dirs := make([]string, 0, b.depth)

	for i := 0; i < b.depth; i++ {
		dirs = append(dirs, filepath[:b.prefixLength])
		filepath = filepath[b.prefixLength:]
	}

	return dirs, filename
}

// Get value by key.
func (b *treeBucket) Get(key []byte) ([]byte, error) {
	dirPaths, filename := b.treePath(key)
	if dirPaths == nil {
		return nil, errShortKey
	}

	p := path.Join(b.dir, path.Join(dirPaths...), filename)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, bucket.ErrNotFound
	}

	return ioutil.ReadFile(p)
}

// Set value by key.
func (b *treeBucket) Set(key, value []byte) error {
	dirPaths, filename := b.treePath(key)
	if dirPaths == nil {
		return errShortKey
	}

	var (
		dirPath = path.Join(dirPaths...)
		p       = path.Join(b.dir, dirPath, filename)
	)

	if err := os.MkdirAll(path.Join(b.dir, dirPath), b.perm); err != nil {
		return err
	}

	err := ioutil.WriteFile(p, value, b.perm)
	if err == nil {
		b.sz.Add(int64(len(value)))
	}

	return err
}

// Del value by key.
func (b *treeBucket) Del(key []byte) error {
	dirPaths, filename := b.treePath(key)
	if dirPaths == nil {
		return errShortKey
	}

	var (
		err error
		fi  os.FileInfo
		p   = path.Join(b.dir, path.Join(dirPaths...), filename)
	)

	if fi, err = os.Stat(p); os.IsNotExist(err) {
		return bucket.ErrNotFound
	} else if err = os.Remove(p); err == nil {
		b.sz.Sub(fi.Size())
	}

	return err
}

// Has checks if key exists.
func (b *treeBucket) Has(key []byte) bool {
	dirPaths, filename := b.treePath(key)
	if dirPaths == nil {
		return false
	}

	p := path.Join(b.dir, path.Join(dirPaths...), filename)

	_, err := os.Stat(p)

	return err == nil
}

// There might be two implementation of listing method: simple with `filepath.Walk()`
// or more complex implementation with path checks, BFS etc. `filepath.Walk()` might
// be slow in large dirs due to sorting operations and non controllable depth.
func (b *treeBucket) listing(root string, fn func(path string, info os.FileInfo) error) error {
	// todo: DFS might be better since it won't store many files in queue.
	// todo: queue length can be specified as a parameter
	q := newQueue(queueCap)
	q.Push(elem{path: root})

	for q.Len() > 0 {
		e := q.Pop()

		s, err := os.Lstat(e.path)
		if err != nil {
			// might be better to log and ignore
			return err
		}

		// check if it is correct file
		if !s.IsDir() {
			// we accept files that located in excepted depth and have correct prefix
			// e.g. file 'abcdef0123' => /ab/cd/abcdef0123
			if e.depth == b.depth+1 && strings.HasPrefix(s.Name(), e.prefix) {
				err = fn(e.path, s)
				if err != nil {
					// might be better to log and ignore
					return err
				}
			}

			continue
		}

		// ignore dirs with inappropriate length or depth
		if e.depth > b.depth || (e.depth > 0 && len(s.Name()) > b.prefixLength) {
			continue
		}

		files, err := readDirNames(e.path)
		if err != nil {
			// might be better to log and ignore
			return err
		}

		for i := range files {
			// add prefix of all dirs in path except root dir
			var prefix string
			if e.depth > 0 {
				prefix = e.prefix + s.Name()
			}

			q.Push(elem{
				depth:  e.depth + 1,
				prefix: prefix,
				path:   path.Join(e.path, files[i]),
			})
		}
	}

	return nil
}

// Size returns the size of the bucket in bytes.
func (b *treeBucket) Size() int64 {
	return b.sz.Load()
}

func (b *treeBucket) size() (size int64) {
	err := b.listing(b.dir, func(_ string, info os.FileInfo) error {
		size += info.Size()
		return nil
	})

	if err != nil {
		size = 0
	}

	return
}

// List all bucket items.
func (b *treeBucket) List() ([][]byte, error) {
	buckets := make([][]byte, 0)

	err := b.listing(b.dir, func(p string, info os.FileInfo) error {
		key, err := decodeHexKey(info.Name())
		if err != nil {
			return err
		}
		buckets = append(buckets, key)
		return nil
	})

	return buckets, err
}

// Filter bucket items by closure.
func (b *treeBucket) Iterate(handler bucket.FilterHandler) error {
	return b.listing(b.dir, func(p string, info os.FileInfo) error {
		val, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}

		key, err := decodeHexKey(info.Name())
		if err != nil {
			return err
		}

		if !handler(key, val) {
			return bucket.ErrIteratingAborted
		}

		return nil
	})
}

// Close bucket (remove all available data).
func (b *treeBucket) Close() error {
	return os.RemoveAll(b.dir)
}

// readDirNames copies `filepath.readDirNames()` without sorting the output.
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	f.Close()

	return names, nil
}
