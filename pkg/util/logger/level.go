package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Level enumerates logging levels representing message severity.
type Level zapcore.Level

// Level values sorted in ascending order of severity.
const (
	LevelDebug = Level(zap.DebugLevel) // verbose series of useful information when debugging the system
	LevelInfo  = Level(zap.InfoLevel)  // general information about what's happening inside the system
	LevelWarn  = Level(zap.WarnLevel)  // non-critical events that should be looked at
	LevelError = Level(zap.ErrorLevel) // critical events that require immediate attention
)

// String returns Level human-readable description.
// String implements fmt.Stringer.
func (x Level) String() string {
	return zapcore.Level(x).String()
}

// maps Level to corresponding static methods of zap.Logger.
var mLevelFunc = map[Level]func(*zap.Logger, string, ...zap.Field){
	LevelDebug: (*zap.Logger).Debug,
	LevelInfo:  (*zap.Logger).Info,
	LevelWarn:  (*zap.Logger).Warn,
	LevelError: (*zap.Logger).Error,
}
