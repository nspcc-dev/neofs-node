package test

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const sampling = 1000

// NewTestLogger creates test logger.
func NewTestLogger(debug bool) *zap.Logger {
	if debug {
		cfg := zap.NewDevelopmentConfig()
		cfg.Sampling = &zap.SamplingConfig{
			Initial:    sampling,
			Thereafter: sampling,
		}

		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

		log, err := cfg.Build()
		if err != nil {
			panic("could not prepare logger: " + err.Error())
		}

		return log
	}

	return zap.L()
}
