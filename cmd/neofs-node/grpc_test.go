package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_calculateMaxReplicationRequestSize(t *testing.T) {
	limit, _ := calculateMaxReplicationRequestSize(64 << 20)
	require.EqualValues(t, 67126272, limit)
	t.Run("int overflow", func(t *testing.T) {
		limit, overflow := calculateMaxReplicationRequestSize(math.MaxInt - 17<<10 + 1)
		require.Negative(t, limit)
		require.EqualValues(t, uint64(math.MaxInt)+1, overflow)
	})
	t.Run("uint64 overflow", func(t *testing.T) {
		limit, overflow := calculateMaxReplicationRequestSize(math.MaxUint64 - 17<<10 + 1)
		require.Negative(t, limit)
		require.EqualValues(t, uint64(math.MaxUint64), overflow)
	})
	t.Run("smaller than gRPC default", func(t *testing.T) {
		limit, _ := calculateMaxReplicationRequestSize(0)
		require.EqualValues(t, 4<<20, limit)
		limit, _ = calculateMaxReplicationRequestSize(4<<20 - 17<<10 - 1)
		require.EqualValues(t, 4<<20, limit)
	})
}

func Test_cfg_handleNewMaxObjectPayloadSize(t *testing.T) {
	var c cfg
	c.log = zap.NewNop()
	c.cfgGRPC.maxRecvMsgSize.Store(0) // any

	c.handleNewMaxObjectPayloadSize(100 << 20)
	require.EqualValues(t, 100<<20+17<<10, c.cfgGRPC.maxRecvMsgSize.Load())
	c.handleNewMaxObjectPayloadSize(64 << 20)
	require.EqualValues(t, 64<<20+17<<10, c.cfgGRPC.maxRecvMsgSize.Load())
	// int overflow
	c.handleNewMaxObjectPayloadSize(math.MaxInt - 17<<10 + 1)
	require.EqualValues(t, math.MaxInt, c.cfgGRPC.maxRecvMsgSize.Load())
	// uint64 overflow
	c.handleNewMaxObjectPayloadSize(math.MaxUint64 - 17<<10 + 1)
	require.EqualValues(t, math.MaxInt, c.cfgGRPC.maxRecvMsgSize.Load())
	// smaller than gRPC default
	c.handleNewMaxObjectPayloadSize(4<<20 - 17<<10 - 1)
	require.EqualValues(t, 4<<20, c.cfgGRPC.maxRecvMsgSize.Load())
}
