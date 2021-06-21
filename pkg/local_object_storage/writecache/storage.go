package writecache

import (
	"errors"
	"os"
	"path"

	lru "github.com/hashicorp/golang-lru"
	"github.com/hashicorp/golang-lru/simplelru"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// store represents persistent storage with in-memory LRU cache
// for flushed items on top of it.
type store struct {
	flushed simplelru.LRUCache
	db      *bbolt.DB
}

const lruKeysCount = 256 * 1024 * 8

const dbName = "small.bolt"

func (c *cache) openStore() error {
	if err := os.MkdirAll(c.path, os.ModePerm); err != nil {
		return err
	}

	db, err := bbolt.Open(path.Join(c.path, dbName), os.ModePerm, &bbolt.Options{
		NoFreelistSync: true,
		NoSync:         true,
	})
	if err != nil {
		return err
	}

	c.fsTree = &fstree.FSTree{
		Info: fstree.Info{
			Permissions: os.ModePerm,
			RootPath:    c.path,
		},
		Depth:      1,
		DirNameLen: 1,
	}

	_ = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(defaultBucket)
		return err
	})

	c.db = db
	c.flushed, _ = lru.New(lruKeysCount)
	return nil
}

func (s *store) removeFlushedKeys(n int) ([][]byte, [][]byte) {
	var keysMem, keysDisk [][]byte
	for i := 0; i < n; i++ {
		k, v, ok := s.flushed.RemoveOldest()
		if !ok {
			break
		}

		if v.(bool) {
			keysMem = append(keysMem, []byte(k.(string)))
		} else {
			keysDisk = append(keysDisk, []byte(k.(string)))
		}
	}

	return keysMem, keysDisk
}

func (c *cache) evictObjects(putCount int) {
	sum := c.flushed.Len() + putCount
	if sum <= lruKeysCount {
		return
	}

	keysMem, keysDisk := c.store.removeFlushedKeys(sum - lruKeysCount)

	if err := c.deleteFromDB(keysMem); err != nil {
		c.log.Error("error while removing objects from write-cache (database)", zap.Error(err))
	}

	if err := c.deleteFromDisk(keysDisk); err != nil {
		c.log.Error("error while removing objects from write-cache (disk)", zap.Error(err))
	}
}

func (c *cache) deleteFromDB(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	var sz uint64
	err := c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for i := range keys {
			has := b.Get(keys[i])
			if has == nil {
				return object.ErrNotFound
			}
			if err := b.Delete(keys[i]); err != nil {
				return err
			}
			sz += uint64(len(has))
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.dbSize.Sub(sz)
	return nil
}

func (c *cache) deleteFromDisk(keys [][]byte) error {
	var lastErr error

	for i := range keys {
		addr := objectSDK.NewAddress()
		addrStr := string(keys[i])

		if err := addr.Parse(addrStr); err != nil {
			c.log.Error("can't parse address", zap.String("address", addrStr))
			continue
		}

		if err := c.fsTree.Delete(addr); err != nil && !errors.Is(err, fstree.ErrFileNotFound) {
			lastErr = err
			c.log.Error("can't remove object from write-cache", zap.Error(err))
			continue
		}
	}

	return lastErr
}
