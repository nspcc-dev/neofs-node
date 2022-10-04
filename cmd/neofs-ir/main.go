package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

const (
	// ErrorReturnCode returns when application crashed at initialization stage.
	ErrorReturnCode = 1

	// SuccessReturnCode returns when application closed without panic.
	SuccessReturnCode = 0
)

func exitErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(ErrorReturnCode)
	}
}

func main() {
	configFile := flag.String("config", "", "path to config")
	versionFlag := flag.Bool("version", false, "neofs-ir node version")
	flag.Parse()

	if *versionFlag {
		fmt.Print(misc.BuildInfo("NeoFS Inner Ring node"))

		os.Exit(SuccessReturnCode)
	}

	cfg, err := newConfig(*configFile)
	exitErr(err)

	var logLvl logger.Level

	switch strLvl := cfg.GetString("logger.level"); strLvl {
	default:
		exitErr(fmt.Errorf("unsupported logging severity level %s", strLvl))
	case "debug":
		logLvl = logger.LevelDebug
	case "info":
		logLvl = logger.LevelInfo
	case "warn":
		logLvl = logger.LevelWarn
	case "error":
		logLvl = logger.LevelError
	}

	var logCfg logger.Config
	var log logger.Logger

	log.Init(&logCfg)

	logCfg.SetLevel(logLvl)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	intErr := make(chan error) // internal inner ring errors

	httpServers := initHTTPServers(cfg, &log)

	innerRing, err := innerring.New(ctx, &log, cfg, intErr)
	exitErr(err)

	// start HTTP servers
	for i := range httpServers {
		srv := httpServers[i]
		go func() {
			exitErr(srv.Serve())
		}()
	}

	// start inner ring
	err = innerRing.Start(ctx, intErr)
	exitErr(err)

	log.Info("application started",
		logger.FieldString("version", misc.Version))

	select {
	case <-ctx.Done():
	case err := <-intErr:
		log.Info("internal error", logger.FieldString("msg", err.Error()))
	}

	innerRing.Stop()

	// shut down HTTP servers
	for i := range httpServers {
		srv := httpServers[i]

		go func() {
			err := srv.Shutdown()
			if err != nil {
				log.Debug("could not shutdown HTTP server",
					logger.FieldError(err),
				)
			}
		}()
	}

	log.Info("application stopped")
}

func initHTTPServers(cfg *viper.Viper, log *logger.Logger) []*httputil.Server {
	items := []struct {
		cfgPrefix string
		handler   func() http.Handler
	}{
		{"pprof", httputil.Handler},
		{"prometheus", promhttp.Handler},
	}

	httpServers := make([]*httputil.Server, 0, len(items))

	for _, item := range items {
		if !cfg.GetBool(item.cfgPrefix + ".enabled") {
			log.Info(item.cfgPrefix + " is disabled, skip")
			continue
		}

		addr := cfg.GetString(item.cfgPrefix + ".address")

		var prm httputil.Prm

		prm.Address = addr
		prm.Handler = item.handler()

		httpServers = append(httpServers,
			httputil.New(prm,
				httputil.WithShutdownTimeout(
					cfg.GetDuration(item.cfgPrefix+".shutdown_timeout"),
				),
			),
		)
	}

	return httpServers
}
