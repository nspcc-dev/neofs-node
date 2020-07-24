package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"flag"
	"os"
	"time"

	"github.com/nspcc-dev/neofs-api-go/service"
	state2 "github.com/nspcc-dev/neofs-api-go/state"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/worker"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/node"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/network/muxer"
	statesrv "github.com/nspcc-dev/neofs-node/pkg/network/transport/state/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type params struct {
	dig.In

	Debug  profiler.Profiler `optional:"true"`
	Metric profiler.Metrics  `optional:"true"`
	Worker worker.Workers    `optional:"true"`
	Muxer  muxer.Mux
	Logger *zap.Logger
}

var (
	healthCheck bool
	configFile  string
)

func runner(ctx context.Context, p params) error {
	// create combined service, that would start/stop all
	svc := fix.NewServices(p.Debug, p.Metric, p.Muxer, p.Worker)

	p.Logger.Info("start services")
	svc.Start(ctx)

	<-ctx.Done()

	p.Logger.Info("stop services")
	svc.Stop()

	return nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// FIXME: this is a copypaste from node settings constructor
func keyFromCfg(v *viper.Viper) (*ecdsa.PrivateKey, error) {
	switch key := v.GetString("node.private_key"); key {
	case "":
		return nil, errors.New("`node.private_key` could not be empty")
	case "generated":
		return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	default:
		return crypto.LoadPrivateKey(key)
	}
}

func runHealthCheck() {
	if !healthCheck {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := config.NewConfig(config.Params{
		File:    configFile,
		Prefix:  misc.Prefix,
		Name:    misc.NodeName,
		Version: misc.Version,

		AppDefaults: setDefaults,
	})
	check(err)

	addr := cfg.GetString("node.address")

	key, err := keyFromCfg(cfg)
	if err != nil {
		check(err)
	}

	con, err := grpc.DialContext(ctx, addr,
		// TODO: we must provide grpc.WithInsecure() or set credentials
		grpc.WithInsecure())
	check(err)

	req := new(statesrv.HealthRequest)
	req.SetTTL(service.NonForwardingTTL)
	if err := service.SignRequestData(key, req); err != nil {
		check(err)
	}

	res, err := state2.NewStatusClient(con).
		HealthCheck(ctx, req)
	check(errors.Wrapf(err, "address: %q", addr))

	var exitCode int

	if !res.Healthy {
		exitCode = 2
	}
	_, _ = os.Stdout.Write([]byte(res.Status + "\n"))
	os.Exit(exitCode)
}

func main() {
	flag.BoolVar(&healthCheck, "health", healthCheck, "run health-check")

	// todo: if configFile is empty, we can check './config.yml' manually
	flag.StringVar(&configFile, "config", configFile, "use config.yml file")
	flag.Parse()

	runHealthCheck()

	fix.New(&fix.Settings{
		File:    configFile,
		Name:    misc.NodeName,
		Prefix:  misc.Prefix,
		Runner:  runner,
		Build:   misc.Build,
		Version: misc.Version,

		AppDefaults: setDefaults,
	}, node.Module).RunAndCatch()
}
