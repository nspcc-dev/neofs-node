package logger

import (
	"errors"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger represents the component
// for writing messages to log.
//
// It is a type alias of
// go.uber.org/zap.Logger.
type Logger = zap.Logger

// Prm groups Logger's parameters.
type Prm struct {
	level zapcore.Level
}

// ErrNilLogger is returned by functions that
// expect a non-nil Logger, but received nil.
var ErrNilLogger = errors.New("logger is nil")

// SetLevelString sets minimum logging level.
//
// Returns error of s is not a string representation of zap.Level
// value (see zapcore.Level docs).
func (p *Prm) SetLevelString(s string) error {
	return p.level.UnmarshalText([]byte(s))
}

// NewLogger constructs a new zap logger instance.
//
// Logger is built from production logging configuration with:
//  * parameterized level;
//  * console encoding;
//  * ISO8601 time encoding.
//
// Logger records a stack trace for all messages at or above fatal level.
func NewLogger(prm Prm) (*Logger, error) {
	c := zap.NewProductionConfig()
	c.Level = zap.NewAtomicLevelAt(prm.level)
	c.Encoding = "console"
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return c.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
}
