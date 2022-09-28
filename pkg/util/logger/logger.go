package logger

import (
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
// Successful passing non-nil parameters to the NewLogger (if returned
// error is nil) connects the parameters with the returned Logger.
// Parameters that have been connected to the Logger support its
// configuration changing.
//
// Passing Prm after a successful connection via the NewLogger, connects
// the Prm to a new instance of the Logger.
//
// See also Reload, SetLevelString.
type Prm struct {
	// link to the created Logger
	// instance; used for a runtime
	// reconfiguration
	_log *Logger

	// support runtime rereading
	level zapcore.Level

	// do not support runtime rereading
}

// SetLevelString sets the minimum logging level. Default is
// "info".
//
// Returns an error if s is not a string representation of a
// supporting logging level.
//
// Supports runtime rereading.
func (p *Prm) SetLevelString(s string) error {
	return p.level.UnmarshalText([]byte(s))
}

// Reload reloads configuration of a connected instance of the Logger.
// Returns ErrLoggerNotConnected if no connection has been performed.
// Returns any reconfiguration error from the Logger directly.
func (p Prm) Reload() error {
	if p._log == nil {
		// incorrect logger usage
		panic("parameters are not connected to any Logger")
	}

	return p._log.reload(p)
}

func defaultPrm() *Prm {
	return new(Prm)
}

// NewLogger constructs a new zap logger instance. Constructing with nil
// parameters is safe: default values will be used then.
// Passing non-nil parameters after a successful creation (non-error) allows
// runtime reconfiguration.
//
// Logger is built from production logging configuration with:
//   - parameterized level;
//   - console encoding;
//   - ISO8601 time encoding.
//
// Logger records a stack trace for all messages at or above fatal level.
func NewLogger(prm *Prm) (*Logger, error) {
	if prm == nil {
		prm = defaultPrm()
	}

	lvl := zap.NewAtomicLevelAt(prm.level)

	c := zap.NewProductionConfig()
	c.Level = lvl
	c.Encoding = "console"
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	lZap, err := c.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	if err != nil {
		return nil, err
	}

	l := &Logger{Logger: lZap, lvl: lvl}
	prm._log = l

	return l, nil
}

func (l *Logger) reload(prm Prm) error {
	l.lvl.SetLevel(prm.level)
	return nil
}
