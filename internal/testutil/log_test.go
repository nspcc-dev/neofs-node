package testutil_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewBufferedLogger(t *testing.T) {
	cnr := cidtest.ID()
	obj := oidtest.ID()

	const message1 = "foo"
	fields1 := []zap.Field{
		zap.Int("int", 1),
		zap.Duration("dur", 123*time.Millisecond),
		zap.Stringer("dur_str", 123*time.Millisecond),
	}
	e1 := testutil.LogEntry{Level: zap.DebugLevel, Message: message1, Fields: map[string]any{
		"int":     json.Number("1"),
		"dur":     json.Number("0.123"),
		"dur_str": "123ms",
	}}

	const message2 = "bar"
	fields2 := []zap.Field{
		zap.Stringer("cid", cnr),
		zap.Stringer("oid", obj),
	}
	e2 := testutil.LogEntry{Message: message2, Fields: map[string]any{
		"cid": cnr.String(),
		"oid": obj.String(),
	}}

	for _, tc := range []struct {
		level zapcore.Level
		write func(*zap.Logger, string, ...zap.Field)
	}{
		{level: zap.DebugLevel, write: (*zap.Logger).Debug},
		{level: zap.InfoLevel, write: (*zap.Logger).Info},
		{level: zap.WarnLevel, write: (*zap.Logger).Warn},
		{level: zap.ErrorLevel, write: (*zap.Logger).Error},
	} {
		t.Run("level="+tc.level.String(), func(t *testing.T) {
			l, b := testutil.NewBufferedLogger(t, zap.DebugLevel)
			b.AssertEmpty()

			l.Debug(message1, fields1...)

			b.AssertEqual([]testutil.LogEntry{e1})
			b.AssertContains(e1)
			b.AssertSingle(e1)

			tc.write(l, message2, fields2...)

			e2.Level = tc.level

			b.AssertEqual([]testutil.LogEntry{e1, e2})
			b.AssertContains(e1)
			b.AssertContains(e2)
		})
	}
}
