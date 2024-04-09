package metrics_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestNewNodeMetrics(t *testing.T) {
	require.NotPanics(t, func() {
		_ = metrics.NewNodeMetrics("any_version")
	})
}
