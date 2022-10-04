package test

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// NewLogger creates a new logger.
//
// If debug, development logger is created.
func NewLogger(debug bool) *logger.Logger {
	if !debug {
		return logger.Nop()
	}

	var l logger.Logger
	var c logger.Config

	l.Init(&c)

	c.SetLevel(logger.LevelDebug)

	return &l
}
