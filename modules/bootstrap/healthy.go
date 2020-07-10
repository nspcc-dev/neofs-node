package bootstrap

import (
	"crypto/ecdsa"
	"sync"

	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/placement"
	"github.com/nspcc-dev/neofs-node/services/public/state"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	healthyParams struct {
		dig.In

		Logger   *zap.Logger
		Viper    *viper.Viper
		Place    placement.Component
		Checkers []state.HealthChecker `group:"healthy"`

		// for ChangeState
		PrivateKey *ecdsa.PrivateKey

		MorphNetmapContract *implementations.MorphNetmapContract
	}

	healthyResult struct {
		dig.Out

		HealthyClient HealthyClient

		StateService state.Service
	}

	// HealthyClient is an interface of healthiness checking tool.
	HealthyClient interface {
		Healthy() error
	}

	healthyClient struct {
		*sync.RWMutex
		healthy func() error
	}
)

const (
	errUnhealthy = internal.Error("unhealthy")
)

func (h *healthyClient) setHandler(handler func() error) {
	if handler == nil {
		return
	}

	h.Lock()
	h.healthy = handler
	h.Unlock()
}

func (h *healthyClient) Healthy() error {
	if h.healthy == nil {
		return errUnhealthy
	}

	return h.healthy()
}

func newHealthy(p healthyParams) (res healthyResult, err error) {
	sp := state.Params{
		Stater:              p.Place,
		Logger:              p.Logger,
		Viper:               p.Viper,
		Checkers:            p.Checkers,
		PrivateKey:          p.PrivateKey,
		MorphNetmapContract: p.MorphNetmapContract,
	}

	if res.StateService, err = state.New(sp); err != nil {
		return
	}

	healthyClient := &healthyClient{
		RWMutex: new(sync.RWMutex),
	}

	healthyClient.setHandler(res.StateService.Healthy)

	res.HealthyClient = healthyClient

	return
}
