package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
)

const (
	// ErrorReturnCode returns when application crashed at initialization stage.
	ErrorReturnCode = 1

	// SuccessReturnCode returns when application closed without panic.
	SuccessReturnCode = 0
)

// exits with ErrorReturnCode if err != nil.
func exitErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(ErrorReturnCode)
	}
}

// exits with ErrorReturnCode or SuccessReturnCode depending on err.
func exitWithCode(err error) {
	if err != nil {
		os.Exit(ErrorReturnCode)
	}

	os.Exit(SuccessReturnCode)
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

	logLevel, err := zap.ParseAtomicLevel(cfg.Logger.Level)
	exitErr(err)

	c := zap.NewProductionConfig()
	c.Level = logLevel
	c.Encoding = cfg.Logger.Encoding
	c.Sampling = nil
	if (term.IsTerminal(int(os.Stdout.Fd())) && !cfg.IsSet("logger.timestamp")) || cfg.Logger.Timestamp {
		c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		c.EncoderConfig.EncodeTime = func(_ time.Time, _ zapcore.PrimitiveArrayEncoder) {}
	}

	log, err := c.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	exitErr(err)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	intErr := make(chan error) // internal inner ring errors

	httpServers := initHTTPServers(cfg, log)
	// start HTTP servers
	for i := range httpServers {
		srv := httpServers[i]
		go func() {
			exitErr(srv.ListenAndServe())
		}()
	}

	innerRing, err := innerring.New(ctx, log, cfg, intErr)
	exitErr(err)

	// start inner ring
	err = innerRing.Start(ctx, intErr)
	exitErr(err)

	log.Info("application started",
		zap.String("version", misc.Version))

	select {
	case <-ctx.Done():
	case err = <-intErr:
		log.Info("internal error", zap.String("msg", err.Error()))
	}

	innerRing.Stop()

	// shut down HTTP servers
	var shutdownWG errgroup.Group
	for i := range httpServers {
		srv := httpServers[i]

		shutdownWG.Go(func() error {
			err := srv.Shutdown()
			if err != nil {
				log.Debug("could not shutdown HTTP server",
					zap.Error(err),
				)
			}

			return err
		})
	}

	shutdownErr := shutdownWG.Wait()
	if err == nil && shutdownErr != nil {
		err = shutdownErr
	}

	log.Info("application stopped")

	exitWithCode(err)
}

func initHTTPServers(cfg *config.Config, log *zap.Logger) []*httputil.Server {
	items := []struct {
		service config.BasicService
		name    string
		handler func() http.Handler
	}{
		{cfg.Prometheus, "prometheus", promhttp.Handler},
		{cfg.Pprof, "pprof", httputil.Handler},
	}

	httpServers := make([]*httputil.Server, 0, len(items))

	for _, item := range items {
		if !item.service.Enabled {
			log.Info(item.name + " is disabled, skip")
			continue
		}
		log.Info(item.name + " is enabled")

		addr := item.service.Address

		var prm httputil.Prm

		prm.Address = addr
		prm.Handler = item.handler()

		httpServers = append(httpServers,
			httputil.New(prm,
				httputil.WithShutdownTimeout(
					item.service.ShutdownTimeout,
				),
			),
		)
	}

	return httpServers
}
