package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const (
	// ErrorReturnCode returns when application crashed at initialization stage
	ErrorReturnCode = 1

	// SuccessReturnCode returns when application closed without panic
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
	validators := flag.String("vote", "", "hex encoded public keys split with comma")
	versionFlag := flag.Bool("version", false, "neofs-ir node version")
	flag.Parse()

	if *versionFlag {
		fmt.Println("version:", misc.Version)
		os.Exit(SuccessReturnCode)
	}

	cfg, err := newConfig(*configFile)
	exitErr(err)

	var logPrm logger.Prm

	err = logPrm.SetLevelString(
		cfg.GetString("logger.level"),
	)
	exitErr(err)

	log, err := logger.NewLogger(logPrm)
	exitErr(err)

	ctx := grace.NewGracefulContext(log)
	intErr := make(chan error) // internal inner ring errors

	httpServers := initHTTPServers(cfg)

	innerRing, err := innerring.New(ctx, log, cfg)
	if err != nil {
		exitErr(err)
	}

	if len(*validators) != 0 {
		validatorKeys, err := parsePublicKeysFromString(*validators)
		exitErr(err)

		err = innerRing.InitAndVoteForSidechainValidator(validatorKeys)
		exitErr(err)

		return
	}

	// start HTTP servers
	for i := range httpServers {
		srv := httpServers[i]
		go func() {
			exitErr(srv.Serve())
		}()
	}

	// start inner ring
	err = innerRing.Start(ctx, intErr)
	if err != nil {
		exitErr(err)
	}

	log.Info("application started",
		zap.String("build time", misc.Build),
		zap.String("version", misc.Version),
		zap.String("debug", misc.Debug),
	)

	select {
	case <-ctx.Done():
	case err := <-intErr:
		// todo: restart application instead of shutdown
		log.Info("internal error", zap.String("msg", err.Error()))
	}

	innerRing.Stop()

	// shut down HTTP servers
	for i := range httpServers {
		srv := httpServers[i]

		go func() {
			err := srv.Shutdown()
			if err != nil {
				log.Debug("could not shutdown HTTP server",
					zap.String("error", err.Error()),
				)
			}
		}()
	}

	log.Info("application stopped")
}

func parsePublicKeysFromString(argument string) (keys.PublicKeys, error) {
	publicKeysString := strings.Split(argument, ",")

	return innerring.ParsePublicKeysFromStrings(publicKeysString)
}

func initHTTPServers(cfg *viper.Viper) []*httputil.Server {
	var httpServers []*httputil.Server

	for _, item := range []struct {
		cfgPrefix string
		handler   func() http.Handler
	}{
		{"profiler", httputil.Handler},
		{"metrics", promhttp.Handler},
	} {
		addr := cfg.GetString(item.cfgPrefix + ".address")
		if addr == "" {
			continue
		}

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
