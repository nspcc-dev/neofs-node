package peers

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/nspcc-dev/neofs-node/lib/transport"
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
		Shutdown() error
		Job(context.Context)
		Address() multiaddr.Multiaddr
		RemoveConnection(maddr multiaddr.Multiaddr) error
		Listen(maddr multiaddr.Multiaddr) (manet.Listener, error)
		Connect(ctx context.Context, maddr multiaddr.Multiaddr) (manet.Conn, error)
		GRPCConnector
	}

	// GRPCConnector is an interface of gRPC virtual connector.
	GRPCConnector interface {
		GRPCConnection(ctx context.Context, maddr multiaddr.Multiaddr, reset bool) (*grpc.ClientConn, error)
	}

	// Params groups the parameters of Interface.
	Params struct {
		Address          multiaddr.Multiaddr
		Transport        transport.Transport
		Logger           *zap.Logger
		Attempts         int64
		AttemptsTTL      time.Duration
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
		addr multiaddr.Multiaddr // self address
		tr   transport.Transport
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

		cons struct {
			*sync.RWMutex
			items map[string]transport.Connection
		}

		lis struct {
			*sync.RWMutex
			items map[string]manet.Listener
		}
	}
)

const (
	defaultAttemptsCount    = 5
	defaultAttemptsTTL      = 30 * time.Second
	defaultCloseTimer       = 30 * time.Second
	defaultConIdleTTL       = 30 * time.Second
	defaultKeepAliveTTL     = 5 * time.Second
	defaultMetricsTimeout   = 5 * time.Second
	defaultKeepAlivePingTTL = 50 * time.Millisecond
)

var (
	// ErrDialToSelf is returned if we attempt to dial our own peer
	ErrDialToSelf = errors.New("dial to self attempted")
	// ErrEmptyAddress returns when you try to create Interface with empty address
	ErrEmptyAddress = errors.New("self address could not be empty")
	// ErrEmptyTransport returns when you try to create Interface with empty transport
	ErrEmptyTransport = errors.New("transport could not be empty")
)

var errNilMultiaddr = errors.New("empty multi-address")

func (s *iface) Shutdown() error {
	s.lis.Lock()
	s.cons.Lock()
	s.grpc.globalMutex.Lock()

	defer func() {
		s.lis.Unlock()
		s.cons.Unlock()
		s.grpc.globalMutex.Unlock()
	}()

	for addr := range s.cons.items {
		if err := s.removeNetConnection(addr); err != nil {
			return errors.Wrapf(err, "could not remove net connection `%s`", addr)
		}
	}

	for addr := range s.grpc.connBook {
		if err := s.removeGRPCConnection(addr); err != nil {
			return errors.Wrapf(err, "could not remove net connection `%s`", addr)
		}
	}

	for addr := range s.lis.items {
		if err := s.removeListener(addr); err != nil {
			return errors.Wrapf(err, "could not remove listener `%s`", addr)
		}
	}

	return nil
}

// RemoveConnection from Interface.
// Used only in tests, consider removing.
func (s *iface) RemoveConnection(maddr multiaddr.Multiaddr) error {
	addr, err := convertAddress(maddr)
	if err != nil {
		return err
	}

	s.cons.Lock()
	s.grpc.globalMutex.Lock()

	defer func() {
		s.cons.Unlock()
		s.grpc.globalMutex.Unlock()
	}()

	// Try to remove connection
	if err := s.removeNetConnection(maddr.String()); err != nil {
		return errors.Wrapf(err, "could not remove net connection `%s`", maddr.String())
	}

	// Try to remove gRPC connection
	if err := s.removeGRPCConnection(addr); err != nil {
		return errors.Wrapf(err, "could not remove gRPC connection `%s`", addr)
	}

	// TODO remove another connections

	return nil
}

func (s *iface) removeListener(addr string) error {
	if lis, ok := s.lis.items[addr]; ok {
		if err := lis.Close(); err != nil {
			return err
		}

		delete(s.lis.items, addr)
	}

	return nil
}

func (s *iface) removeNetConnection(addr string) error {
	// Try to remove simple connection
	if con, ok := s.cons.items[addr]; ok {
		if err := con.Close(); err != nil {
			return err
		}

		delete(s.cons.items, addr)
	}

	return nil
}

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

// Connect to address
// Used only in tests, consider removing.
func (s *iface) Connect(ctx context.Context, maddr multiaddr.Multiaddr) (manet.Conn, error) {
	var (
		err error
		con transport.Connection
	)

	if maddr.Equal(s.addr) {
		return nil, ErrDialToSelf
	}

	s.cons.RLock()
	con, ok := s.cons.items[maddr.String()]
	s.cons.RUnlock()

	if ok && !con.Closed() {
		return con, nil
	}

	if con, err = s.newConnection(ctx, maddr, false); err != nil {
		return nil, err
	}

	s.cons.Lock()
	s.cons.items[maddr.String()] = con
	s.cons.Unlock()

	return con, nil
}

// Listen try to find listener or creates new.
func (s *iface) Listen(maddr multiaddr.Multiaddr) (manet.Listener, error) {
	// fixme: concurrency issue there, same as 5260f04d
	//        but it's not so bad, because `Listen()` used
	//        once during startup routine.
	s.lis.RLock()
	lis, ok := s.lis.items[maddr.String()]
	s.lis.RUnlock()

	if ok {
		return lis, nil
	}

	lis, err := s.tr.Listen(maddr)
	if err != nil {
		return nil, err
	}

	s.lis.Lock()
	s.lis.items[maddr.String()] = lis
	s.lis.Unlock()

	return lis, nil
}

// Address of current Interface instance.
func (s *iface) Address() multiaddr.Multiaddr {
	return s.addr
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

func (s *iface) newConnection(ctx context.Context, addr multiaddr.Multiaddr, reset bool) (transport.Connection, error) {
	return s.tr.Dial(ctx, addr, reset)
}

func gRPCKeepAlive(ping, ttl time.Duration) grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                ping,
		Timeout:             ttl,
		PermitWithoutStream: true,
	})
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
func (s *iface) GRPCConnection(ctx context.Context, maddr multiaddr.Multiaddr, reset bool) (*grpc.ClientConn, error) {
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
		gRPCKeepAlive(s.pingTTL, s.keepAlive),
		// TODO: we must provide grpc.WithInsecure() or set credentials
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return s.newConnection(ctx, maddr, reset)
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
	if p.Address == nil {
		return nil, ErrEmptyAddress
	}

	if p.Transport == nil {
		return nil, ErrEmptyTransport
	}

	if p.Attempts <= 0 {
		p.Attempts = defaultAttemptsCount
	}

	if p.AttemptsTTL <= 0 {
		p.AttemptsTTL = defaultAttemptsTTL
	}

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

		log:  p.Logger,
		addr: p.Address,
		tr:   p.Transport,
		grpc: struct {
			globalMutex *sync.RWMutex
			bookMutex   *sync.RWMutex
			connBook    map[string]*connItem
		}{
			globalMutex: new(sync.RWMutex),
			bookMutex:   new(sync.RWMutex),
			connBook:    make(map[string]*connItem),
		},
		cons: struct {
			*sync.RWMutex
			items map[string]transport.Connection
		}{
			RWMutex: new(sync.RWMutex),
			items:   make(map[string]transport.Connection),
		},
		lis: struct {
			*sync.RWMutex
			items map[string]manet.Listener
		}{
			RWMutex: new(sync.RWMutex),
			items:   make(map[string]manet.Listener),
		},
	}, nil
}
