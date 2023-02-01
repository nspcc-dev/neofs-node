package pilorama

import (
	"sort"
	"sync"
	"time"

	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
)

type batch struct {
	forest *boltForest
	timer  *time.Timer
	// mtx protects timer and operations fields.
	// Because mtx can be taken inside a transaction,
	// transactions MUST NOT be executed with the mutex taken to avoid a deadlock.
	mtx        sync.Mutex
	start      sync.Once
	cid        cidSDK.ID
	treeID     string
	results    []chan<- error
	operations []*Move
}

func (b *batch) trigger() {
	b.mtx.Lock()
	if b.timer != nil {
		b.timer.Stop()
	}
	b.mtx.Unlock()
	b.start.Do(b.run)
}

func (b *batch) run() {
	fullID := bucketName(b.cid, b.treeID)
	err := b.forest.db.Update(func(tx *bbolt.Tx) error {
		bLog, bTree, err := b.forest.getTreeBuckets(tx, fullID)
		if err != nil {
			return err
		}

		b.mtx.Lock()
		b.timer = nil
		b.mtx.Unlock()

		// Sorting without a mutex is ok, because we append to this slice only if timer is non-nil.
		// See (*boltForest).addBatch for details.
		sort.Slice(b.operations, func(i, j int) bool {
			return b.operations[i].Time < b.operations[j].Time
		})

		var lm LogMove
		return b.forest.applyOperation(bLog, bTree, b.operations, &lm)
	})
	for i := range b.operations {
		b.results[i] <- err
	}
}
