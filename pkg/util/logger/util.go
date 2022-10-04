package logger

import "go.uber.org/zap"

var nop = &Logger{log: zap.NewNop()}

// Nop returns no-op Logger which never writes out logs. It is useful as a
// default setting of the required Logger.
func Nop() *Logger {
	return nop
}
