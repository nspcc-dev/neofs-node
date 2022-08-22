package storagetest

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/stretchr/testify/require"
)

// Component represents single storage component.
type Component interface {
	Open(bool) error
	SetMode(mode.Mode) error
	Init() error
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
		require.NoError(t, s.Init())
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
	require.NoError(t, s.Init())
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
		require.NoError(t, s.Init())
		require.NoError(t, s.SetMode(m))
	})
}

func TestModeTransition(t *testing.T, cons Constructor, from, to mode.Mode) {
	// Use-case: normal node operation.
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	require.NoError(t, s.SetMode(from))
	require.NoError(t, s.SetMode(to))
	require.NoError(t, s.Close())
}
