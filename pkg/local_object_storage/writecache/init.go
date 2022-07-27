package writecache

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

func (c *cache) initFlushMarks() {
	c.log.Info("filling flush marks for objects in FSTree")

	var prm fstree.IterationPrm
	prm.WithLazyHandler(func(addr oid.Address, _ func() ([]byte, error)) error {
		if c.isFlushed(addr) {
			c.store.flushed.Add(addr.EncodeToString(), true)
		}
		return nil
	})
	_ = c.fsTree.Iterate(prm)

	c.log.Info("filling flush marks for objects in database")

	var m []string
	var lastKey []byte
	var batchSize = flushBatchSize
	for {
		m = m[:0]

		// We put objects in batches of fixed size to not interfere with main put cycle a lot.
		_ = c.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			cs := b.Cursor()
			for k, _ := cs.Seek(lastKey); k != nil && len(m) < batchSize; k, _ = cs.Next() {
				m = append(m, string(k))
			}
			return nil
		})

		var addr oid.Address
		for i := range m {
			if err := addr.DecodeString(m[i]); err != nil {
				continue
			}

			if c.isFlushed(addr) {
				c.store.flushed.Add(addr.EncodeToString(), true)
			}
		}

		if len(m) == 0 {
			break
		}
		lastKey = append([]byte(m[len(m)-1]), 0)
	}

	c.log.Info("finished updating flush marks")
}

func (c *cache) isFlushed(addr oid.Address) bool {
	var existsPrm meta.ExistsPrm
	existsPrm.SetAddress(addr)

	mRes, err := c.metabase.Exists(existsPrm)
	if err != nil {
		return errors.Is(err, object.ErrObjectIsExpired) || errors.As(err, new(apistatus.ObjectAlreadyRemoved))
	}

	if !mRes.Exists() {
		return false
	}

	var prm blobstor.ExistsPrm
	prm.SetAddress(addr)

	res, err := c.blobstor.Exists(prm)
	return err == nil && res.Exists()
}
