package fix

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-node/lib/fix/config"
	"github.com/nspcc-dev/neofs-node/lib/fix/logger"
	"github.com/nspcc-dev/neofs-node/lib/fix/module"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	// App is an interface of executable application.
	App interface {
		Run() error
		RunAndCatch()
	}

	app struct {
		err    error
		log    *zap.Logger
		di     *dig.Container
		runner interface{}
	}

	// Settings groups the application parameters.
	Settings struct {
		File    string
		Type    string
		Name    string
		Prefix  string
		Build   string
		Version string
		Runner  interface{}

		AppDefaults func(v *viper.Viper)
	}
)

func (a *app) RunAndCatch() {
	err := a.Run()

	if errors.Is(err, context.Canceled) {
		return
	}

	if ok, _ := strconv.ParseBool(misc.Debug); ok {
		a.CatchTrace(err)
	}

	a.Catch(err)
}

func (a *app) Run() error {
	if a.err != nil {
		return a.err
	}

	// setup app logger:
	if err := a.di.Invoke(func(l *zap.Logger) {
		a.log = l
	}); err != nil {
		return err
	}

	return a.di.Invoke(a.runner)
}

// New is an application constructor.
func New(s *Settings, mod module.Module) App {
	var (
		a   app
		err error
	)

	a.di = dig.New(dig.DeferAcyclicVerification())
	a.runner = s.Runner

	if s.Prefix == "" {
		s.Prefix = s.Name
	}

	mod = mod.Append(
		module.Module{
			{Constructor: logger.NewLogger},
			{Constructor: NewGracefulContext},
			{Constructor: func() (*viper.Viper, error) {
				return config.NewConfig(config.Params{
					File:    s.File,
					Type:    s.Type,
					Prefix:  strings.ToUpper(s.Prefix),
					Name:    s.Name,
					Version: fmt.Sprintf("%s(%s)", s.Version, s.Build),

					AppDefaults: s.AppDefaults,
				})
			}},
		})

	if err = module.Provide(a.di, mod); err != nil {
		a.err = err
	}

	return &a
}
