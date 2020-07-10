package core

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type testBucket struct {
	Bucket

	items []BucketItem
}

func (s *testBucket) Iterate(f FilterHandler) error {
	for i := range s.items {
		if !f(s.items[i].Key, s.items[i].Val) {
			return ErrIteratingAborted
		}
	}

	return nil
}

func TestListBucketItems(t *testing.T) {
	_, err := ListBucketItems(nil, nil)
	require.EqualError(t, err, errEmptyBucket.Error())

	b := new(testBucket)

	_, err = ListBucketItems(b, nil)
	require.EqualError(t, err, ErrNilFilterHandler.Error())

	var (
		count = 10
		ln    = 10
		items = make([]BucketItem, 0, count)
	)

	for i := 0; i < count; i++ {
		items = append(items, BucketItem{
			Key: testData(t, ln),
			Val: testData(t, ln),
		})
	}

	b.items = items

	res, err := ListBucketItems(b, func(key, val []byte) bool { return true })
	require.NoError(t, err)
	require.Equal(t, items, res)

	res, err = ListBucketItems(b, func(key, val []byte) bool { return false })
	require.NoError(t, err)
	require.Empty(t, res)
}

func testData(t *testing.T, sz int) []byte {
	d := make([]byte, sz)
	_, err := rand.Read(d)
	require.NoError(t, err)

	return d
}
