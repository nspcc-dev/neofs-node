package peers

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type (
	// Interface is an interface of network connections controller.
	Interface interface {
		Job(context.Context)
		GRPCConnector
	}

	// GRPCConnector is an interface of gRPC virtual connector.
	GRPCConnector interface {
		GRPCConnection(ctx context.Context, maddr multiaddr.Multiaddr) (*grpc.ClientConn, error)
	}

	// Params groups the parameters of Interface.
	Params struct {
		Logger           *zap.Logger
		ConnectionTTL    time.Duration
		ConnectionIDLE   time.Duration
		MetricsTimeout   time.Duration
		KeepAliveTTL     time.Duration
		KeepAlivePingTTL time.Duration
	}

	connItem struct {
		sync.RWMutex
		conn *grpc.ClientConn
		used time.Time
	}

	iface struct {
		log  *zap.Logger
		tick time.Duration
		idle time.Duration

		keepAlive time.Duration
		pingTTL   time.Duration

		metricsTimeout time.Duration

		grpc struct {
			// globalMutex used by garbage collector and other high
			globalMutex *sync.RWMutex
			// bookMutex resolves concurrent access to the new connection
			bookMutex *sync.RWMutex
			// connBook contains connection info
			// it's mutex resolves concurrent access to existed connection
			connBook map[string]*connItem
		}
	}
)

const (
	defaultCloseTimer       = 30 * time.Second
	defaultConIdleTTL       = 30 * time.Second
	defaultKeepAliveTTL     = 5 * time.Second
	defaultMetricsTimeout   = 5 * time.Second
	defaultKeepAlivePingTTL = 50 * time.Millisecond
)

var errNilMultiaddr = errors.New("empty multi-address")

func (s *iface) removeGRPCConnection(addr string) error {
	if gCon, ok := s.grpc.connBook[addr]; ok && gCon.conn != nil {
		if err := gCon.conn.Close(); err != nil {
			state, ok := status.FromError(err)
			if !ok {
				return err
			}

			s.log.Debug("error state",
				zap.String("address", addr),
				zap.Any("code", state.Code()),
				zap.String("state", state.Message()),
				zap.Any("details", state.Details()))
		}
	}

	delete(s.grpc.connBook, addr)

	return nil
}

func isGRPCClosed(con *grpc.ClientConn) bool {
	switch con.GetState() {
	case connectivity.Idle, connectivity.Connecting, connectivity.Ready:
		return false
	default:
		// connectivity.TransientFailure, connectivity.Shutdown
		return true
	}
}

func convertAddress(maddr multiaddr.Multiaddr) (string, error) {
	if maddr == nil {
		return "", errNilMultiaddr
	}

	addr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return "", errors.Wrapf(err, "could not convert address `%s`", maddr)
	}

	return addr.String(), nil
}

// GRPCConnection creates gRPC connection over peers connection.
func (s *iface) GRPCConnection(ctx context.Context, maddr multiaddr.Multiaddr) (*grpc.ClientConn, error) {
	addr, err := convertAddress(maddr)
	if err != nil {
		return nil, errors.Wrapf(err, "could not convert `%v`", maddr)
	}

	// Get global mutex on read.
	// All high level function e.g. peers garbage collector
	// or shutdown must use globalMutex.Lock instead
	s.grpc.globalMutex.RLock()

	// Get connection item from connection book or create a new one.
	// Concurrent map access resolved by bookMutex.
	s.grpc.bookMutex.Lock()

	item, ok := s.grpc.connBook[addr]
	if !ok {
		item = new(connItem)
		s.grpc.connBook[addr] = item
	}

	s.grpc.bookMutex.Unlock()

	// Now lock connection item.
	// This denies concurrent access to the same address,
	// but allows concurrent access to a different addresses.
	item.Lock()

	if item.conn != nil && !isGRPCClosed(item.conn) {
		item.used = time.Now()

		item.Unlock()
		s.grpc.globalMutex.RUnlock()

		return item.conn, nil
	}

	// Если вышеописанные строки переместить внутрь WithDialer,
	// мы получим сломанный коннекшн, но ошибка не будет возвращена,
	// поэтому мы сначала проверяем коннекшн и лишь потом возвращаем
	// *gRPC.ClientConn
	//
	// Это будет работать с `grpc.WithBlock()`, см. ниже
	conn, err := grpc.DialContext(ctx, maddr.String(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                s.pingTTL,
			Timeout:             s.keepAlive,
			PermitWithoutStream: true,
		}),
		// TODO: we must provide grpc.WithInsecure() or set credentials
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return network.Dial(ctx, maddr)
		}),
	)
	if err == nil {
		item.conn = conn
		item.used = time.Now()
	}

	item.Unlock()
	s.grpc.globalMutex.RUnlock()

	return conn, err
}

// New create iface instance and check arguments.
func New(p Params) (Interface, error) {
	if p.ConnectionTTL <= 0 {
		p.ConnectionTTL = defaultCloseTimer
	}

	if p.ConnectionIDLE <= 0 {
		p.ConnectionIDLE = defaultConIdleTTL
	}

	if p.KeepAliveTTL <= 0 {
		p.KeepAliveTTL = defaultKeepAliveTTL
	}

	if p.KeepAlivePingTTL <= 0 {
		p.KeepAlivePingTTL = defaultKeepAlivePingTTL
	}

	if p.MetricsTimeout <= 0 {
		p.MetricsTimeout = defaultMetricsTimeout
	}

	return &iface{
		tick: p.ConnectionTTL,
		idle: p.ConnectionIDLE,

		keepAlive: p.KeepAliveTTL,
		pingTTL:   p.KeepAlivePingTTL,

		metricsTimeout: p.MetricsTimeout,

		log: p.Logger,
		grpc: struct {
			globalMutex *sync.RWMutex
			bookMutex   *sync.RWMutex
			connBook    map[string]*connItem
		}{
			globalMutex: new(sync.RWMutex),
			bookMutex:   new(sync.RWMutex),
			connBook:    make(map[string]*connItem),
		},
	}, nil
}
