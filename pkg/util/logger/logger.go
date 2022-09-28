package logger

import (
	"errors"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger represents a component
// for writing messages to log.
type Logger struct {
	*zap.Logger
	lvl zap.AtomicLevel
}

// Prm groups Logger's parameters.
type Prm struct {
	level zapcore.Level
}

// ErrNilLogger is returned by functions that
// expect a non-nil Logger but received nil.
var ErrNilLogger = errors.New("logger is nil")

// SetLevelString sets the minimum logging level.
//
// Returns error of s is not a string representation of zap.Level
// value (see zapcore.Level docs).
func (p *Prm) SetLevelString(s string) error {
	return p.level.UnmarshalText([]byte(s))
}

// NewLogger constructs a new zap logger instance.
//
// Logger is built from production logging configuration with:
//   - parameterized level;
//   - console encoding;
//   - ISO8601 time encoding.
//
// Logger records a stack trace for all messages at or above fatal level.
func NewLogger(prm Prm) (*Logger, error) {
	lvl := zap.NewAtomicLevelAt(prm.level)

	c := zap.NewProductionConfig()
	c.Level = lvl
	c.Encoding = "console"
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := c.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	if err != nil {
		return nil, err
	}

	return &Logger{Logger: l, lvl: lvl}, nil
}
