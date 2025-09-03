package testutil

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

const (
	logLevelKey   = "level"
	logMessageKey = "msg"
	logTimeKey    = "ts"
)

// LogEntry represents single [zap.Logger] entry.
type LogEntry struct {
	Level   zapcore.Level
	Message string
	// Integer values are represented as [json.Number].
	Fields map[string]any
}

// LogBuffer is a memory buffer for [zap.Logger] entries.
type LogBuffer struct {
	t testing.TB
	l sync.Locker
	b zaptest.Buffer
}

// NewBufferedLogger returns buffered logger for testing.
//
// Entries with severity less than minLevel are never written.
func NewBufferedLogger(t testing.TB, minLevel zapcore.Level) (*zap.Logger, *LogBuffer) {
	// TODO: https://github.com/nspcc-dev/neofs-node/issues/3313
	var lb LogBuffer
	lb.t = t
	lb.l = new(sync.Mutex)

	encCfg := zap.NewProductionEncoderConfig()
	encCfg.LevelKey = logLevelKey
	encCfg.MessageKey = logMessageKey
	encCfg.TimeKey = logTimeKey

	zc := zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zap.CombineWriteSyncers(lockedWriteSyncer{
			l: lb.l,
			w: &lb.b,
		}),
		minLevel,
	)

	return zap.New(zc), &lb
}

// AssertEmpty asserts that log is empty.
func (x *LogBuffer) AssertEmpty() {
	x.AssertEqual(nil)
}

// AssertSingle asserts that log has given entry only.
func (x *LogBuffer) AssertSingle(e LogEntry) {
	x.AssertEqual([]LogEntry{e})
}

// AssertEqual asserts that log consists of given ordered entries.
func (x *LogBuffer) AssertEqual(es []LogEntry) {
	got := x.collectEntries()
	require.Len(x.t, got, len(es))
	for i := range es {
		require.Equal(x.t, es[i], got[i], i)
	}
}

// AssertContains asserts that log contains at least one entry with given message.
func (x *LogBuffer) AssertContains(e LogEntry) {
	require.Contains(x.t, x.collectEntries(), e)
}

func (x *LogBuffer) collectEntries() []LogEntry {
	x.l.Lock()
	lines := x.b.Lines()
	x.l.Unlock()
	res := make([]LogEntry, len(lines))

	var err error
	for i := range lines {
		dec := json.NewDecoder(strings.NewReader(lines[i]))
		dec.UseNumber()

		var m map[string]any
		require.NoError(x.t, dec.Decode(&m), i)

		v, ok := m[logLevelKey]
		require.True(x.t, ok, i)
		require.IsType(x.t, "", v)

		res[i].Level, err = zapcore.ParseLevel(v.(string))
		require.NoError(x.t, err, i)

		v, ok = m[logMessageKey]
		require.True(x.t, ok, i)
		require.IsType(x.t, "", v)
		res[i].Message = v.(string)

		delete(m, logTimeKey)
		delete(m, logLevelKey)
		delete(m, logMessageKey)
		res[i].Fields = m
	}

	return res
}

// [zapcore.Lock] implementation analogue.
type lockedWriteSyncer struct {
	l sync.Locker
	w zapcore.WriteSyncer
}

func (x lockedWriteSyncer) Write(p []byte) (int, error) {
	x.l.Lock()
	n, err := x.w.Write(p)
	x.l.Unlock()
	return n, err
}

func (x lockedWriteSyncer) Sync() error {
	x.l.Lock()
	err := x.w.Sync()
	x.l.Unlock()
	return err
}
