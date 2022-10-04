package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config provides dynamic logging configuration. See methods for details.
//
// For correct operation, Config instance MUST be constructed initialized
// using Logger.Init.
type Config struct {
	level zap.AtomicLevel
}

// SetLevel sets the minimum logging level. All messages with lower priority
// level than the configured one are not recorded in the log. For example,
// LevelDebug messages are not logged if LevelInfo min priority is set.
//
// Level MUST be named enum value.
func (x *Config) SetLevel(lvl Level) {
	_, ok := mLevelFunc[lvl]
	if !ok {
		panic(fmt.Sprintf("unsupported level value %v", lvl))
	}

	x.level.SetLevel(zapcore.Level(lvl))
}
