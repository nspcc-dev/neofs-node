package getsvc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func BenchmarkExecCtxSetLoggerProduction(b *testing.B) {
	l, err := zap.NewProduction()
	require.NoError(b, err)
	require.NotEqual(b, l.Level(), zap.DebugLevel)

	var c execCtx

	b.ResetTimer()

	for range b.N {
		c.setLogger(l)
	}
}
