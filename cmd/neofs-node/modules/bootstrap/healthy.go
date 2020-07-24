package bootstrap

import (
	"crypto/ecdsa"
	"errors"
	"sync"

	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	state "github.com/nspcc-dev/neofs-node/pkg/network/transport/state/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
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

		Client *contract.Wrapper
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

var errUnhealthy = errors.New("unhealthy")

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
		Stater:     p.Place,
		Logger:     p.Logger,
		Viper:      p.Viper,
		Checkers:   p.Checkers,
		PrivateKey: p.PrivateKey,
		Client:     p.Client,
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
