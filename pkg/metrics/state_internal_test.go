package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPolicerHeadMetrics(t *testing.T) {
	m := newStateMetrics()

	m.HandlePolicerHead(PolicerHeadOK, time.Millisecond)
	m.HandlePolicerHead(PolicerHeadTimeout, 5*time.Second)
	m.HandlePolicerHead(PolicerHeadTimeout, 5*time.Second)
	require.EqualValues(t, 1, value(t, m.policerHeadCounter.WithLabelValues(PolicerHeadOK)))
	require.EqualValues(t, 2, value(t, m.policerHeadCounter.WithLabelValues(PolicerHeadTimeout)))
	require.Zero(t, value(t, m.policerHeadCounter.WithLabelValues(PolicerHeadError)))
	require.EqualValues(t, 3, value(t, m.policerHeadDuration))
}

// value returns the value of a single counter or gauge, or the number of
// observations of a single histogram.
func value(t *testing.T, c prometheus.Collector) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, mfs, 1)
	require.Len(t, mfs[0].GetMetric(), 1)
	m := mfs[0].GetMetric()[0]
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	default:
		return float64(m.GetHistogram().GetSampleCount())
	}
}
