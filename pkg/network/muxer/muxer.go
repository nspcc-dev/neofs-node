package muxer

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/soheilhy/cmux"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// StoreParams groups the parameters of network connections muxer constructor.
	Params struct {
		Logger      *zap.Logger
		API         *fasthttp.Server
		Address     multiaddr.Multiaddr
		ShutdownTTL time.Duration
		P2P         *grpc.Server
	}

	// Mux is an interface of network connections muxer.
	Mux interface {
		Start(ctx context.Context)
		Stop()
	}

	muxer struct {
		maddr multiaddr.Multiaddr
		run   *int32
		lis   net.Listener
		log   *zap.Logger
		ttl   time.Duration

		p2p *grpc.Server
		api *fasthttp.Server

		done chan struct{}
	}
)

const (
	// we close listener, that's why we ignore this errors
	errClosedConnection = "use of closed network connection"
	errMuxListenerClose = "mux: listener closed"
	errHTTPServerClosed = "http: Server closed"
)

var (
	ignoredErrors = []string{
		errClosedConnection,
		errMuxListenerClose,
		errHTTPServerClosed,
	}
)

// New constructs network connections muxer and returns Mux interface.
func New(p Params) Mux {
	return &muxer{
		maddr: p.Address,
		ttl:   p.ShutdownTTL,
		run:   new(int32),
		api:   p.API,
		p2p:   p.P2P,
		log:   p.Logger,

		done: make(chan struct{}),
	}
}

func needCatch(err error) bool {
	if err == nil || containsErr(err) {
		return false
	}

	return true
}

func containsErr(err error) bool {
	for _, msg := range ignoredErrors {
		if strings.Contains(err.Error(), msg) {
			return true
		}
	}

	return false
}

func (m *muxer) Start(ctx context.Context) {
	var err error

	// if already started - ignore
	if !atomic.CompareAndSwapInt32(m.run, 0, 1) {
		m.log.Warn("already started")
		return
	} else if m.lis != nil {
		m.log.Info("try close old listener")
		if err = m.lis.Close(); err != nil {
			m.log.Fatal("could not close old listener",
				zap.Error(err))
		}
	}

	if m.lis, err = network.Listen(m.maddr); err != nil {
		m.log.Fatal("could not close old listener",
			zap.Error(err))
	}

	mux := cmux.New(m.lis)
	mux.HandleError(func(e error) bool {
		if needCatch(e) {
			m.log.Error("error-handler: something went wrong",
				zap.Error(e))
		}
		return true
	})

	// trpcL := mux.Match(cmux.Any()) // Any means anything that is not yet matched.
	hLis := mux.Match(cmux.HTTP1Fast())
	gLis := mux.Match(cmux.HTTP2())
	pLis := mux.Match(cmux.Any())

	m.log.Debug("delay context worker")

	go func() {
		<-ctx.Done()
		m.Stop()
	}()

	m.log.Debug("delay tcp")

	go func() {
		m.log.Debug("tcp: serve")
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
			}

			con, err := pLis.Accept()
			if err != nil {
				break loop
			}

			_ = con.Close()
		}

		m.log.Debug("tcp: stopped")
	}()

	m.log.Debug("delay p2p")

	go func() {
		if m.p2p == nil {
			m.log.Info("p2p: service is empty")
			return
		}

		m.log.Debug("p2p: serve")

		if err := m.p2p.Serve(gLis); needCatch(err) {
			m.log.Error("p2p: something went wrong",
				zap.Error(err))
		}

		m.log.Debug("p2p: stopped")
	}()

	m.log.Debug("delay api")

	go func() {
		if m.api == nil {
			m.log.Info("api: service is empty")
			return
		}

		m.log.Debug("api: serve")

		if err := m.api.Serve(hLis); needCatch(err) {
			m.log.Error("rpc: something went wrong",
				zap.Error(err))
		}

		m.log.Debug("rpc: stopped")
	}()

	m.log.Debug("delay serve")

	go func() {
		defer func() { close(m.done) }()

		m.log.Debug("mux: serve")

		if err := mux.Serve(); needCatch(err) {
			m.log.Fatal("mux: something went wrong",
				zap.Error(err))
		}

		m.log.Debug("mux: stopped")
	}()
}

func (m *muxer) Stop() {
	if !atomic.CompareAndSwapInt32(m.run, 1, 0) {
		m.log.Warn("already stopped")
		return
	}

	if err := m.lis.Close(); err != nil {
		m.log.Error("could not close connection",
			zap.Error(err))
	}

	m.log.Debug("lis: close ok")

	<-m.done // muxer stopped

	if m.api != nil {
		if err := m.api.Shutdown(); needCatch(err) {
			m.log.Error("api: could not shutdown",
				zap.Error(err))
		}

		m.log.Debug("api: shutdown ok")
	}

	if m.p2p != nil {
		m.p2p.GracefulStop()
		m.log.Debug("p2p: shutdown ok")
	}
}
