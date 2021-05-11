package logger

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger represents the component
// for writing messages to log.
//
// It is a type alias of
// go.uber.org/zap.Logger.
type Logger = zap.Logger

// ErrNilLogger is returned by functions that
// expect a non-nil Logger, but received nil.
var ErrNilLogger = errors.New("logger is nil")

// NewLogger is a logger's constructor.
func NewLogger(v *viper.Viper) (*Logger, error) {
	c := zap.NewProductionConfig()
	c.Level = safeLevel(v.GetString("logger.level"))
	c.Encoding = "console"
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return c.Build(
		// record a stack trace for all messages at or above fatal level
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
}

func safeLevel(lvl string) zap.AtomicLevel {
	switch strings.ToLower(lvl) {
	case "debug":
		return zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "fatal":
		return zap.NewAtomicLevelAt(zap.FatalLevel)
	case "panic":
		return zap.NewAtomicLevelAt(zap.PanicLevel)
	default:
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	}
}
