package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
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

	log, err := logger.NewLogger(cfg)
	exitErr(err)

	ctx := grace.NewGracefulContext(log)
	intErr := make(chan error) // internal inner ring errors

	pprof := profiler.NewProfiler(log, cfg)
	prometheus := profiler.NewMetrics(log, cfg)

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

	// start pprof if enabled
	if pprof != nil {
		pprof.Start(ctx)
		defer pprof.Stop()
	}

	// start prometheus if enabled
	if prometheus != nil {
		prometheus.Start(ctx)
		defer prometheus.Stop()
	}

	// start inner ring
	err = innerRing.Start(ctx, intErr)
	if err != nil {
		exitErr(err)
	}

	log.Info("application started")

	select {
	case <-ctx.Done():
	case err := <-intErr:
		// todo: restart application instead of shutdown
		log.Info("internal error", zap.String("msg", err.Error()))
	}

	innerRing.Stop()

	log.Info("application stopped")
}

func parsePublicKeysFromString(argument string) (keys.PublicKeys, error) {
	publicKeysString := strings.Split(argument, ",")

	return innerring.ParsePublicKeysFromStrings(publicKeysString)
}
