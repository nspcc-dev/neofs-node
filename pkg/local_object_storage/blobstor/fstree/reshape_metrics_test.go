package fstree

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/stretchr/testify/require"
)

type reshapeProgressTracker struct {
	values []float64
}

func (t *reshapeProgressTracker) SetReshapeProgress(progress float64) {
	t.values = append(t.values, progress)
}

func (t *reshapeProgressTracker) recordedValues() []float64 {
	return t.values
}

func newReshapeFSTree(t *testing.T, lastProcessedPath string) (*FSTree, common.ID, *reshapeProgressTracker) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	tracker := new(reshapeProgressTracker)
	fs := New(
		WithPath(dir),
		WithDepth(3),
		WithReshapeProgressTracker(func(string) ReshapeProgressTracker { return tracker }),
	)
	fs.secondaryDepth = 2
	fs.shardID = id
	fs.shardIDSet = true
	fs.reshapeProgressTracker = fs.newReshapeProgressTracker(id.String())
	fs.descriptor = fsDescriptor{
		Version: currentVersion,
		Depth:   2,
		ShardID: id.String(),
		Subtype: SubtypeBlobstor,
		Reshape: &reshapeDescriptor{
			FromDepth:         2,
			ToDepth:           3,
			LastProcessedPath: lastProcessedPath,
		},
	}
	require.NoError(t, writeDescriptor(fs.descriptorPath(), fs.descriptor))

	return fs, id, tracker
}

func TestFSTreeReshapeMetric(t *testing.T) {
	t.Run("percentage", func(t *testing.T) {
		tree := New()
		tree.secondaryDepth = 5

		require.Zero(t, tree.reshapeProgress(""))
		require.Zero(t, tree.reshapeProgress("1/1/1/1/1/object"))
		require.Equal(t, 100.0, tree.reshapeProgress("z/z/z/z/z/object"))
		require.InDelta(t, 50.0, tree.reshapeProgress("V/V/V/V/V/object"), 1)
		require.Zero(t, tree.reshapeProgress("1/1/object"))
		require.Zero(t, tree.reshapeProgress("11/1/1/1/1/object"))
		require.Zero(t, tree.reshapeProgress("0/1/1/1/1/object"))
	})

	t.Run("updated", func(t *testing.T) {
		fs, _, tracker := newReshapeFSTree(t, "")

		require.NoError(t, fs.updateReshapeProgress("z/z/object"))
		require.Equal(t, []float64{100}, tracker.recordedValues())

		require.NoError(t, fs.completeReshape())
		require.Equal(t, []float64{100, 100}, tracker.recordedValues())
	})

	t.Run("resumed", func(t *testing.T) {
		fs, _, tracker := newReshapeFSTree(t, "V/V/object")

		fs.startReshape()
		<-fs.reshapeDone

		values := tracker.recordedValues()
		require.Len(t, values, 2)
		require.InDelta(t, 50.0, values[0], 1)
		require.Equal(t, 100.0, values[1])
	})

	t.Run("tracker factory", func(t *testing.T) {
		id, err := common.NewID()
		require.NoError(t, err)

		tracker := new(reshapeProgressTracker)
		var trackerShardID string
		fs := New(
			WithPath(t.TempDir()),
			WithDepth(2),
			WithReshapeProgressTracker(func(shardID string) ReshapeProgressTracker {
				trackerShardID = shardID
				return tracker
			}),
		)
		require.NoError(t, fs.Init(id))
		t.Cleanup(func() { require.NoError(t, fs.Close()) })

		require.Equal(t, id.String(), trackerShardID)
		require.Same(t, tracker, fs.reshapeProgressTracker)
	})

	t.Run("without tracker", func(t *testing.T) {
		fs, _, _ := newReshapeFSTree(t, "")
		fs.reshapeProgressTracker = nil

		require.NoError(t, fs.updateReshapeProgress("z/z/object"))
	})
}
