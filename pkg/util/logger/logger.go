package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger represents a component for writing messages to log.
//
// Logger prioritizes all messages into different severity levels. See Level
// type for details.
//
// Each log record contains timestamp, level, message and optional structured
// context. Logger MAY also add some helpful info like code location. Output is
// designed for human - rather than machine - consumption. It serializes the
// core log entry data (timestamp, level, etc.) in a plain-text format and
// leaves the structured context as JSON.
//
// For correct operation, Logger instance MUST be initialized using Init.
type Logger struct {
	log *zap.Logger
}

// Init initializes ready-to-go Logger. Bind optional runtime configuration
// component if provided: all subsequent reconfigurations are immediately
// applied to the Logger instance. All Logger and Config methods are safe for
// concurrent use. Default level is LevelInfo. Config SHOULD NOT be used before
// Init successfully completed: all settings are overwritten.
func (x *Logger) Init(cfg *Config) {
	c := zap.NewProductionConfig()
	c.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	c.Encoding = "console"
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var err error

	x.log, err = c.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to build zap logger: %v", err))
	}

	if cfg != nil {
		cfg.level = c.Level
	}
}

func convertContext(ctx []Field) []zap.Field {
	if len(ctx) == 0 {
		return nil
	}

	res := make([]zap.Field, len(ctx))

	for i := 0; i < len(ctx); i++ {
		res[i] = ctx[i].f
	}

	return res
}

func (x *Logger) write(lvl Level, msg string, ctx ...Field) {
	w, ok := mLevelFunc[lvl]
	if !ok {
		panic(fmt.Sprintf("unsupported level value %v", lvl))
	}

	w(x.log, msg, convertContext(ctx)...)
}

// Debug logs a message at LevelDebug level with optional context. If Logger
// has minimum priority setting higher than the LevelDebug, message is not
// recorded. See Config.SetLevel for details.
func (x *Logger) Debug(msg string, ctx ...Field) {
	x.write(LevelDebug, msg, ctx...)
}

// Info behaves like Debug but at LevelInfo level.
func (x *Logger) Info(msg string, ctx ...Field) {
	x.write(LevelInfo, msg, ctx...)
}

// Warn behaves like Debug but at LevelWarn level.
func (x *Logger) Warn(msg string, ctx ...Field) {
	x.write(LevelWarn, msg, ctx...)
}

// Error behaves like Debug but at LevelError level.
func (x *Logger) Error(msg string, ctx ...Field) {
	x.write(LevelError, msg, ctx...)
}

// WithContext returns new Logger instance which inherits all properties
// of the parent Logger, and adds given context to all records.
//
// Parent Logger MUST be correctly initialized.
func (x *Logger) WithContext(ctx ...Field) *Logger {
	if len(ctx) == 0 {
		return x
	}

	return &Logger{
		log: x.log.With(convertContext(ctx)...),
	}
}
