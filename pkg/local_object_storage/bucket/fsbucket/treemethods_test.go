package fsbucket

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func prepareTree(badFiles bool) (string, error) {
	name := make([]byte, 32)
	root, err := ioutil.TempDir("", "treeBucket_test")
	if err != nil {
		return "", err
	}

	// paths must contain strings with hex ascii symbols
	paths := [][]string{
		{root, "abcd"},
		{root, "abcd", "cdef"},
		{root, "abcd", "cd01"},
		{root, "0123", "2345"},
		{root, "0123", "2345", "4567"},
	}

	dirs := make([]string, len(paths))

	for i := range paths {
		dirs[i] = path.Join(paths[i]...)

		err = os.MkdirAll(dirs[i], 0700)
		if err != nil {
			return "", err
		}

		// create couple correct files
		for j := 0; j < 2; j++ {
			_, err := rand.Read(name)
			if err != nil {
				return "", err
			}

			filePrefix := new(strings.Builder)
			for k := 1; k < len(paths[i]); k++ {
				filePrefix.WriteString(paths[i][k])
			}
			filePrefix.WriteString(hex.EncodeToString(name))

			file, err := os.OpenFile(path.Join(dirs[i], filePrefix.String()), os.O_CREATE, 0700)
			if err != nil {
				return "", err
			}
			file.Close()
		}

		if !badFiles {
			continue
		}

		// create one bad file
		_, err := rand.Read(name)
		if err != nil {
			return "", err
		}

		file, err := os.OpenFile(path.Join(dirs[i], "fff"+hex.EncodeToString(name)), os.O_CREATE, 0700)
		if err != nil {
			return "", err
		}
		file.Close()
	}

	return root, nil
}

func TestTreebucket_List(t *testing.T) {
	root, err := prepareTree(true)
	require.NoError(t, err)
	defer os.RemoveAll(root)

	b := treeBucket{
		dir:          root,
		perm:         0700,
		depth:        1,
		prefixLength: 4,
	}
	results, err := b.List()
	require.NoError(t, err)
	require.Len(t, results, 2)

	b.depth = 2
	results, err = b.List()
	require.NoError(t, err)
	require.Len(t, results, 6)

	b.depth = 3
	results, err = b.List()
	require.NoError(t, err)
	require.Len(t, results, 2)

	b.depth = 4
	results, err = b.List()
	require.NoError(t, err)
	require.Len(t, results, 0)
}

func TestTreebucket(t *testing.T) {
	root, err := prepareTree(true)
	require.NoError(t, err)
	defer os.RemoveAll(root)

	b := treeBucket{
		dir:          root,
		perm:         0700,
		depth:        2,
		prefixLength: 4,
		sz:           atomic.NewInt64(0),
	}

	results, err := b.List()
	require.NoError(t, err)
	require.Len(t, results, 6)

	t.Run("Get", func(t *testing.T) {
		for i := range results {
			_, err = b.Get(results[i])
			require.NoError(t, err)
		}
		_, err = b.Get([]byte("Hello world!"))
		require.Error(t, err)
	})

	t.Run("Has", func(t *testing.T) {
		for i := range results {
			require.True(t, b.Has(results[i]))
		}
		require.False(t, b.Has([]byte("Unknown key")))
	})

	t.Run("Set", func(t *testing.T) {
		keyHash := sha256.Sum256([]byte("Set this key"))
		key := keyHash[:]
		value := make([]byte, 32)
		rand.Read(value)

		// set sha256 key
		err := b.Set(key, value)
		require.NoError(t, err)

		require.True(t, b.Has(key))
		data, err := b.Get(key)
		require.NoError(t, err)
		require.Equal(t, data, value)

		filename := hex.EncodeToString(key)
		_, err = os.Lstat(path.Join(root, filename[:4], filename[4:8], filename))
		require.NoError(t, err)

		// set key that cannot be placed in the required dir depth
		key, err = hex.DecodeString("abcdef")
		require.NoError(t, err)

		err = b.Set(key, value)
		require.Error(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		keyHash := sha256.Sum256([]byte("Delete this key"))
		key := keyHash[:]
		value := make([]byte, 32)
		rand.Read(value)

		err := b.Set(key, value)
		require.NoError(t, err)

		// delete sha256 key
		err = b.Del(key)
		require.NoError(t, err)

		_, err = b.Get(key)
		require.Error(t, err)
		filename := hex.EncodeToString(key)
		_, err = os.Lstat(path.Join(root, filename[:4], filename[4:8], filename))
		require.Error(t, err)
	})
}

func TestTreebucket_Close(t *testing.T) {
	root, err := prepareTree(true)
	require.NoError(t, err)
	defer os.RemoveAll(root)

	b := treeBucket{
		dir:          root,
		perm:         0700,
		depth:        2,
		prefixLength: 4,
	}
	err = b.Close()
	require.NoError(t, err)

	_, err = os.Lstat(root)
	require.Error(t, err)
}

func TestTreebucket_Size(t *testing.T) {
	root, err := prepareTree(true)
	require.NoError(t, err)
	defer os.RemoveAll(root)

	var size int64 = 1024
	key := []byte("Set this key")
	value := make([]byte, size)
	rand.Read(value)

	b := treeBucket{
		dir:          root,
		perm:         0700,
		depth:        2,
		prefixLength: 4,
		sz:           atomic.NewInt64(0),
	}

	err = b.Set(key, value)
	require.NoError(t, err)
	require.Equal(t, size, b.Size())
}

func BenchmarkTreebucket_List(b *testing.B) {
	root, err := prepareTree(false)
	defer os.RemoveAll(root)
	if err != nil {
		b.Error(err)
	}

	treeFSBucket := &treeBucket{
		dir:          root,
		perm:         0755,
		depth:        2,
		prefixLength: 4,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := treeFSBucket.List()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkFilewalkBucket_List(b *testing.B) {
	root, err := prepareTree(false)
	defer os.RemoveAll(root)
	if err != nil {
		b.Error(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buckets := make([]bucket.BucketItem, 0)

		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}

			val, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}

			key, err := decodeHexKey(info.Name())
			if err != nil {
				return err
			}

			buckets = append(buckets, bucket.BucketItem{
				Key: key,
				Val: val,
			})

			return nil
		})
	}
}

func BenchmarkTreeBucket_Size(b *testing.B) {
	root, err := prepareTree(false)
	defer os.RemoveAll(root)
	if err != nil {
		b.Error(err)
	}

	treeFSBucket := &treeBucket{
		dir:          root,
		perm:         0755,
		depth:        2,
		prefixLength: 4,
	}

	treeFSBucket.sz = atomic.NewInt64(treeFSBucket.size())

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = treeFSBucket.Size()
	}
}
