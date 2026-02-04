package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveShard(t *testing.T) {
	const numOfShards = 6

	e := testNewEngineWithShardNum(t, numOfShards)
	t.Cleanup(func() {
		e.Close()
	})

	require.Equal(t, numOfShards, len(e.shards))

	removedNum := numOfShards / 2

	mSh := make(map[string]bool, numOfShards)
	for i, sh := range e.DumpInfo().Shards {
		if i == removedNum {
			break
		}

		mSh[sh.ID.String()] = true
	}

	for id, remove := range mSh {
		if remove {
			e.removeShards(id)
		}
	}

	require.Equal(t, numOfShards-removedNum, len(e.shards))

	for id, removed := range mSh {
		_, ok := e.shards[id]
		require.True(t, ok != removed)
	}
}
