package state

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/state"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// Service is an interface of the server of State service.
	Service interface {
		state.StatusServer
		grpc.Service
		Healthy() error
	}

	// HealthChecker is an interface of node healthiness checking tool.
	HealthChecker interface {
		Name() string
		Healthy() bool
	}

	// Stater is an interface of the node's network state storage with read access.
	Stater interface {
		NetworkState() *bootstrap.SpreadMap
	}

	// Params groups the parameters of State service server's constructor.
	Params struct {
		Stater Stater

		Logger *zap.Logger

		Viper *viper.Viper

		Checkers []HealthChecker

		PrivateKey *ecdsa.PrivateKey

		MorphNetmapContract *implementations.MorphNetmapContract
	}

	stateService struct {
		state    Stater
		config   *viper.Viper
		checkers []HealthChecker
		private  *ecdsa.PrivateKey
		owners   map[refs.OwnerID]struct{}

		stateUpdater *implementations.MorphNetmapContract
	}

	// HealthRequest is a type alias of
	// HealthRequest from state package of neofs-api-go.
	HealthRequest = state.HealthRequest
)

const (
	errEmptyViper         = internal.Error("empty config")
	errEmptyLogger        = internal.Error("empty logger")
	errEmptyStater        = internal.Error("empty stater")
	errUnknownChangeState = internal.Error("received unknown state")
)

const msgMissingRequestInitiator = "missing request initiator"

var requestVerifyFunc = core.VerifyRequestWithSignatures

// New is an State service server's constructor.
func New(p Params) (Service, error) {
	switch {
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.Viper == nil:
		return nil, errEmptyViper
	case p.Stater == nil:
		return nil, errEmptyStater
	case p.PrivateKey == nil:
		return nil, crypto.ErrEmptyPrivateKey
	}

	svc := &stateService{
		config:   p.Viper,
		state:    p.Stater,
		private:  p.PrivateKey,
		owners:   fetchOwners(p.Logger, p.Viper),
		checkers: make([]HealthChecker, 0, len(p.Checkers)),

		stateUpdater: p.MorphNetmapContract,
	}

	for i, checker := range p.Checkers {
		if checker == nil {
			p.Logger.Debug("ignore empty checker",
				zap.Int("index", i))
			continue
		}

		p.Logger.Info("register health-checker",
			zap.String("name", checker.Name()))

		svc.checkers = append(svc.checkers, checker)
	}

	return svc, nil
}

func fetchOwners(l *zap.Logger, v *viper.Viper) map[refs.OwnerID]struct{} {
	// if config.yml used:
	items := v.GetStringSlice("node.rpc.owners")

	for i := 0; ; i++ {
		item := v.GetString("node.rpc.owners." + strconv.Itoa(i))

		if item == "" {
			l.Info("stat: skip empty owner", zap.Int("idx", i))
			break
		}

		items = append(items, item)
	}

	result := make(map[refs.OwnerID]struct{}, len(items))

	for i := range items {
		var owner refs.OwnerID

		if data, err := hex.DecodeString(items[i]); err != nil {
			l.Warn("stat: skip wrong hex data",
				zap.Int("idx", i),
				zap.String("key", items[i]),
				zap.Error(err))

			continue
		} else if key := crypto.UnmarshalPublicKey(data); key == nil {
			l.Warn("stat: skip wrong key",
				zap.Int("idx", i),
				zap.String("key", items[i]))
			continue
		} else if owner, err = refs.NewOwnerID(key); err != nil {
			l.Warn("stat: skip wrong key",
				zap.Int("idx", i),
				zap.String("key", items[i]),
				zap.Error(err))
			continue
		}

		result[owner] = struct{}{}

		l.Info("rpc owner added", zap.Stringer("owner", owner))
	}

	return result
}

func nonForwarding(ttl uint32) error {
	if ttl != service.NonForwardingTTL {
		return status.Error(codes.InvalidArgument, service.ErrInvalidTTL.Error())
	}

	return nil
}

func requestInitiator(req service.SignKeyPairSource) *ecdsa.PublicKey {
	if signKeys := req.GetSignKeyPairs(); len(signKeys) > 0 {
		return signKeys[0].GetPublicKey()
	}

	return nil
}

// ChangeState allows to change current node state of node.
// To permit access, used server config options.
// The request should be signed.
func (s *stateService) ChangeState(ctx context.Context, in *state.ChangeStateRequest) (*state.ChangeStateResponse, error) {
	// verify request structure
	if err := requestVerifyFunc(in); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// verify change state permission
	if key := requestInitiator(in); key == nil {
		return nil, status.Error(codes.InvalidArgument, msgMissingRequestInitiator)
	} else if owner, err := refs.NewOwnerID(key); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if _, ok := s.owners[owner]; !ok {
		return nil, status.Error(codes.PermissionDenied, service.ErrWrongOwner.Error())
	}

	// convert State field to NodeState
	if in.GetState() != state.ChangeStateRequest_Offline {
		return nil, status.Error(codes.InvalidArgument, errUnknownChangeState.Error())
	}

	// set update state parameters
	p := implementations.UpdateStateParams{}
	p.SetState(implementations.StateOffline)
	p.SetKey(
		crypto.MarshalPublicKey(&s.private.PublicKey),
	)

	if err := s.stateUpdater.UpdateState(p); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}

	return new(state.ChangeStateResponse), nil
}

// DumpConfig request allows dumping settings for the current node.
// To permit access, used server config options.
// The request should be signed.
func (s *stateService) DumpConfig(_ context.Context, req *state.DumpRequest) (*state.DumpResponse, error) {
	if err := service.ProcessRequestTTL(req, nonForwarding); err != nil {
		return nil, err
	} else if err = requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if key := requestInitiator(req); key == nil {
		return nil, status.Error(codes.InvalidArgument, msgMissingRequestInitiator)
	} else if owner, err := refs.NewOwnerID(key); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if _, ok := s.owners[owner]; !ok {
		return nil, status.Error(codes.PermissionDenied, service.ErrWrongOwner.Error())
	}

	return state.EncodeConfig(s.config)
}

// Netmap returns SpreadMap from Stater (IRState / Place-component).
func (s *stateService) Netmap(_ context.Context, req *state.NetmapRequest) (*bootstrap.SpreadMap, error) {
	if err := service.ProcessRequestTTL(req); err != nil {
		return nil, err
	} else if err = requestVerifyFunc(req); err != nil {
		return nil, err
	}

	if s.state != nil {
		return s.state.NetworkState(), nil
	}

	return nil, status.New(codes.Unavailable, "service unavailable").Err()
}

func (s *stateService) healthy() error {
	for _, svc := range s.checkers {
		if !svc.Healthy() {
			return errors.Errorf("service(%s) unhealthy", svc.Name())
		}
	}

	return nil
}

// Healthy returns error as status of service, if nil service healthy.
func (s *stateService) Healthy() error { return s.healthy() }

// Check that all checkers is healthy.
func (s *stateService) HealthCheck(_ context.Context, req *HealthRequest) (*state.HealthResponse, error) {
	if err := service.ProcessRequestTTL(req); err != nil {
		return nil, err
	} else if err = requestVerifyFunc(req); err != nil {
		return nil, err
	}

	var (
		err  = s.healthy()
		resp = &state.HealthResponse{Healthy: true, Status: "OK"}
	)

	if err != nil {
		resp.Healthy = false
		resp.Status = err.Error()
	}

	return resp, nil
}

func (*stateService) Metrics(_ context.Context, req *state.MetricsRequest) (*state.MetricsResponse, error) {
	if err := service.ProcessRequestTTL(req); err != nil {
		return nil, err
	} else if err = requestVerifyFunc(req); err != nil {
		return nil, err
	}

	return state.EncodeMetrics(prometheus.DefaultGatherer)
}

func (s *stateService) DumpVars(_ context.Context, req *state.DumpVarsRequest) (*state.DumpVarsResponse, error) {
	if err := service.ProcessRequestTTL(req, nonForwarding); err != nil {
		return nil, err
	} else if err = requestVerifyFunc(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if key := requestInitiator(req); key == nil {
		return nil, status.Error(codes.InvalidArgument, msgMissingRequestInitiator)
	} else if owner, err := refs.NewOwnerID(key); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	} else if _, ok := s.owners[owner]; !ok {
		return nil, status.Error(codes.PermissionDenied, service.ErrWrongOwner.Error())
	}

	return state.EncodeVariables(), nil
}

// Name of the service.
func (*stateService) Name() string { return "StatusService" }

// Register service on gRPC server.
func (s *stateService) Register(g *grpc.Server) { state.RegisterStatusServer(g, s) }
