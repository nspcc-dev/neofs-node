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

const (
	formatJSON    = "json"
	formatConsole = "console"

	defaultSamplingInitial    = 100
	defaultSamplingThereafter = 100
)

// ErrNilLogger is returned by functions that
// expect a non-nil Logger, but received nil.
var ErrNilLogger = errors.New("logger is nil")

// NewLogger is a logger's constructor.
func NewLogger(v *viper.Viper) (*Logger, error) {
	c := zap.NewProductionConfig()

	c.OutputPaths = []string{"stdout"}
	c.ErrorOutputPaths = []string{"stdout"}

	if v.IsSet("logger.sampling") {
		c.Sampling = &zap.SamplingConfig{
			Initial:    defaultSamplingInitial,
			Thereafter: defaultSamplingThereafter,
		}

		if val := v.GetInt("logger.sampling.initial"); val > 0 {
			c.Sampling.Initial = val
		}

		if val := v.GetInt("logger.sampling.thereafter"); val > 0 {
			c.Sampling.Thereafter = val
		}
	}

	// logger level
	c.Level = safeLevel(v.GetString("logger.level"))
	traceLvl := safeLevel(v.GetString("logger.trace_level"))

	// logger format
	switch f := v.GetString("logger.format"); strings.ToLower(f) {
	case formatConsole:
		c.Encoding = formatConsole
	default:
		c.Encoding = formatJSON
	}

	// logger time
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := c.Build(
		// enable trace only for current log-level
		zap.AddStacktrace(traceLvl))
	if err != nil {
		return nil, err
	}

	if v.GetBool("logger.no_disclaimer") {
		return l, nil
	}

	name := v.GetString("app.name")
	version := v.GetString("app.version")

	return l.With(
		zap.String("app_name", name),
		zap.String("app_version", version)), nil
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
