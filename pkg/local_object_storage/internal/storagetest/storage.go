package storagetest

import (
	"encoding/binary"
	"io"
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"github.com/stretchr/testify/require"
)

// Component represents single storage component.
type Component interface {
	Open(bool) error
	SetMode(mode.Mode) error
	Init(blobstor.ID) error
	Close() error
}

// Constructor constructs storage component.
// Each call must create a component using different file-system path.
type Constructor = func(t *testing.T) Component

// TestAll checks that storage component doesn't panic under
// any circumstances during shard operation.
func TestAll(t *testing.T, cons Constructor) {
	modes := []mode.Mode{
		mode.ReadWrite,
		mode.ReadOnly,
		mode.Degraded,
		mode.DegradedReadOnly,
	}

	t.Run("close after open", func(t *testing.T) {
		TestCloseAfterOpen(t, cons)
	})
	t.Run("close twice", func(t *testing.T) {
		TestCloseTwice(t, cons)
	})
	t.Run("set mode", func(t *testing.T) {
		for _, m := range modes {
			t.Run(m.String(), func(t *testing.T) {
				TestSetMode(t, cons, m)
			})
		}
	})
	t.Run("mode transition", func(t *testing.T) {
		for _, from := range modes {
			for _, to := range modes {
				TestModeTransition(t, cons, from, to)
			}
		}
	})
}

// TestCloseAfterOpen checks that `Close` can be done right after `Open`.
// Use-case: open shard, encounter error, close before the initialization.
func TestCloseAfterOpen(t *testing.T, cons Constructor) {
	t.Run("RW", func(t *testing.T) {
		// Use-case: irrecoverable error on some components, close everything.
		s := cons(t)
		require.NoError(t, s.Open(false))
		require.NoError(t, s.Close())
	})
	t.Run("RO", func(t *testing.T) {
		// Use-case: irrecoverable error on some components, close everything.
		// Open in read-only must be done after the db is here.
		s := cons(t)
		require.NoError(t, s.Open(false))
		require.NoError(t, s.Init(blobstor.ID{}))
		require.NoError(t, s.Close())

		require.NoError(t, s.Open(true))
		require.NoError(t, s.Close())
	})
}

// TestCloseTwice checks that `Close` can be done twice.
func TestCloseTwice(t *testing.T, cons Constructor) {
	// Use-case: move to maintenance mode twice, first time failed.
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init(blobstor.ID{}))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close()) // already closed, no-op
}

// TestSetMode checks that any mode transition can be done safely.
func TestSetMode(t *testing.T, cons Constructor, m mode.Mode) {
	t.Run("before init", func(t *testing.T) {
		// Use-case: metabase `Init` failed,
		// call `SetMode` on all not-yet-initialized components.
		s := cons(t)
		require.NoError(t, s.Open(false))
		require.NoError(t, s.SetMode(m))

		t.Run("after open in RO", func(t *testing.T) {
			require.NoError(t, s.Close())
			require.NoError(t, s.Open(true))
			require.NoError(t, s.SetMode(m))
		})

		require.NoError(t, s.Close())
	})
	t.Run("after init", func(t *testing.T) {
		s := cons(t)
		// Use-case: notmal node operation.
		require.NoError(t, s.Open(false))
		require.NoError(t, s.Init(blobstor.ID{}))
		require.NoError(t, s.SetMode(m))
	})
}

func TestModeTransition(t *testing.T, cons Constructor, from, to mode.Mode) {
	// Use-case: normal node operation.
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init(blobstor.ID{}))
	require.NoError(t, s.SetMode(from))
	require.NoError(t, s.SetMode(to))
	require.NoError(t, s.Close())
}

// ConcatHeaderAndPayload concatenates given object header with payload in
// Protocol Buffers V3 format.
func ConcatHeaderAndPayload(header []byte, payload []byte) []byte {
	if len(payload) == 0 {
		return header
	}
	payloadLenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(payloadLenBuf, uint64(len(payload)))
	return slices.Concat(header, []byte{iprotobuf.TagBytes4}, payloadLenBuf[:n], payload)
}

// AssertWriteStreamAlreadyAborted asserts that stream methods fail due to already aborted stream.
func AssertWriteStreamAlreadyAborted(t *testing.T, stream io.WriteCloser) {
	_, err := stream.Write([]byte{0})
	require.EqualError(t, err, "stream already aborted")
	err = stream.Close()
	require.EqualError(t, err, "stream already aborted")
}
