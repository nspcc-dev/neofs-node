package session

import (
	"github.com/nspcc-dev/neofs-api-go/session"
	libgrpc "github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	sessionService struct {
		ts  TokenStore
		log *zap.Logger

		epochReceiver EpochReceiver
	}

	// Service is an interface of the server of Session service.
	Service interface {
		libgrpc.Service
		session.SessionServer
	}

	// EpochReceiver is an interface of the container of epoch number with read access.
	EpochReceiver interface {
		Epoch() uint64
	}

	// Params groups the parameters of Session service server's constructor.
	Params struct {
		TokenStore TokenStore

		Logger *zap.Logger

		EpochReceiver EpochReceiver
	}

	// TokenStore is a type alias of
	// TokenStore from session package of neofs-api-go.
	TokenStore = session.PrivateTokenStore

	// CreateRequest is a type alias of
	// CreateRequest from session package of neofs-api-go.
	CreateRequest = session.CreateRequest

	// CreateResponse is a type alias of
	// CreateResponse from session package of neofs-api-go.
	CreateResponse = session.CreateResponse
)

// New is an Session service server's constructor.
func New(p Params) Service {
	return &sessionService{
		ts:  p.TokenStore,
		log: p.Logger,

		epochReceiver: p.EpochReceiver,
	}
}

func (sessionService) Name() string {
	return "Session Server"
}

func (s sessionService) Register(srv *grpc.Server) {
	session.RegisterSessionServer(srv, s)
}
