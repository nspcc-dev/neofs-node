package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
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

	pprof := profiler.NewProfiler(log, cfg)
	prometheus := profiler.NewMetrics(log, cfg)

	innerRing, err := innerring.New(ctx, log, cfg)
	if err != nil {
		exitErr(err)
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
	err = innerRing.Start(ctx)
	if err != nil {
		exitErr(err)
	}

	log.Info("application started")

	// todo: select ctx.Done or exported error channel
	<-ctx.Done()

	innerRing.Stop()

	log.Info("application stopped")
}
