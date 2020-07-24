package muxer

import (
	"context"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/pkg/errors"
	"github.com/soheilhy/cmux"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type (
	errListener struct {
		net.TCPListener
	}

	syncListener struct {
		sync.Mutex
		net.Listener
	}

	errMuxer struct {
		handleError func(error) bool
	}

	testWriter struct{}

	// service is used to implement GreaterServer.
	service struct{}
)

const MIMEApplicationJSON = "application/json"

// Hello is simple handler
func (*service) Hello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	return &HelloResponse{
		Message: "Hello " + req.Name,
	}, nil
}

func (testWriter) Sync() error                       { return nil }
func (testWriter) Write(p []byte) (n int, err error) { return len(p), nil }

func (errMuxer) Match(...cmux.Matcher) net.Listener {
	return &errListener{}
}

func (errMuxer) MatchWithWriters(...cmux.MatchWriter) net.Listener {
	return &errListener{}
}

func (errMuxer) Serve() error {
	return errors.New("cmux.Serve error")
}

func (e *errMuxer) HandleError(h cmux.ErrorHandler) {
	e.handleError = h
}

func (errMuxer) SetReadTimeout(time.Duration) {
	panic("implement me")
}

func (l *syncListener) Close() error {
	l.Lock()
	err := l.Listener.Close()
	l.Unlock()
	return err
}

func (errListener) Close() error { return errors.New("close error") }

func testMultiAddr(is *require.Assertions) multiaddr.Multiaddr {
	mAddr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/0")
	is.NoError(err)
	return mAddr
}

func testLogger() *zap.Logger {
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), testWriter{}, zap.DPanicLevel)
	return zap.New(core).WithOptions()
}

func testHTTPServer() *fasthttp.Server {
	return &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {}}
}

func TestSuite(t *testing.T) {
	t.Run("it should run, stop and not panic", func(t *testing.T) {
		var (
			is  = require.New(t)
			v   = viper.New()
			g   = grpc.NewServer()
			l   = testLogger()
			a   = testMultiAddr(is)
			s   = time.Second
			err error
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		v.SetDefault("api.address", "/ip4/0.0.0.0/tcp/0")
		v.SetDefault("api.shutdown_timeout", time.Second)

		m := New(Params{
			Logger:      l,
			Address:     a,
			ShutdownTTL: s,
			API:         testHTTPServer(),
			P2P:         g,
		})

		is.NotPanics(func() {
			m.Start(ctx)
		})

		res, err := http.Post("http://"+m.(*muxer).lis.Addr().String(), MIMEApplicationJSON, strings.NewReader(`{
				"jsonrpc": "2.0",
				"id": 1
				"method": "get_version",
				"params": [],
			}`))
		is.NoError(err)
		defer res.Body.Close()

		time.Sleep(100 * time.Millisecond)

		is.NotPanics(m.Stop)
	})

	t.Run("it should work with gRPC", func(t *testing.T) {
		var (
			is  = require.New(t)
			g   = grpc.NewServer()
			l   = testLogger()
			s   = time.Second
			err error
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		addr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/63090")
		is.NoError(err)

		RegisterGreeterServer(g, &service{})

		m := New(Params{
			Logger:      l,
			Address:     addr,
			ShutdownTTL: s,
			P2P:         g,
		})

		is.NotPanics(func() {
			m.Start(ctx)
		})

		a, err := manet.ToNetAddr(addr)
		require.NoError(t, err)

		con, err := grpc.DialContext(ctx, a.String(), grpc.WithInsecure())
		require.NoError(t, err)

		res, err := NewGreeterClient(con).Hello(ctx, &HelloRequest{Name: "test"})
		is.NoError(err)
		is.Contains(res.Message, "test")

		time.Sleep(100 * time.Millisecond)

		is.NotPanics(m.Stop)
	})

	t.Run("it should not start if already started", func(t *testing.T) {
		var (
			is = require.New(t)
			g  = grpc.NewServer()
			l  = testLogger()
			a  = testMultiAddr(is)
			s  = time.Second
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		m := New(Params{
			Logger:      l,
			Address:     a,
			ShutdownTTL: s,
			API:         testHTTPServer(),
			P2P:         g,
		})
		is.NotNil(m)

		mux, ok := m.(*muxer)
		is.True(ok)
		is.NotNil(mux)

		*mux.run = 1

		is.NotPanics(func() {
			mux.Start(ctx)
		})

		*mux.run = 0

		is.NotPanics(mux.Stop)
	})

	t.Run("it should fail on close listener", func(t *testing.T) {
		var (
			is = require.New(t)
			g  = grpc.NewServer()
			l  = testLogger()
			a  = testMultiAddr(is)
			s  = time.Second
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		m := New(Params{
			Logger:      l,
			Address:     a,
			ShutdownTTL: s,
			API:         testHTTPServer(),
			P2P:         g,
		})
		is.NotNil(m)

		mux, ok := m.(*muxer)
		is.True(ok)
		is.NotNil(mux)

		mux.lis = &errListener{}

		exit := atomic.NewInt32(0)

		monkey.Patch(os.Exit, func(v int) { exit.Store(int32(v)) })

		is.NotPanics(func() {
			mux.Start(ctx)
		})
		is.Equal(int32(1), exit.Load())
	})

	t.Run("it should fail on create/close Listener without handlers", func(t *testing.T) {
		var (
			is  = require.New(t)
			l   = testLogger()
			err error
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mux := new(muxer)
		mux.log = l
		mux.run = new(int32)
		mux.done = make(chan struct{})
		mux.maddr, err = multiaddr.NewMultiaddr("/ip4/1.1.1.1/tcp/2")
		is.NoError(err)

		mux.lis, err = net.ListenTCP("tcp", nil)
		is.NoError(err)

		exit := atomic.NewInt32(0)
		monkey.Patch(os.Exit, func(v int) {
			exit.Store(int32(v))
		})

		m := &errMuxer{handleError: func(e error) bool { return true }}
		monkey.Patch(cmux.New, func(net.Listener) cmux.CMux {
			// prevent panic:
			mux.lis, err = net.ListenTCP("tcp", nil)
			return m
		})

		mux.Start(ctx)
		// c.So(mux.Start, ShouldNotPanic)

		m.handleError(errors.New("test"))

		is.Equal(int32(1), exit.Load())

		mux.lis = &errListener{}
		*mux.run = 1

		is.NotPanics(mux.Stop)
	})

	t.Run("it should fail on create/close Listener with handlers", func(t *testing.T) {
		var (
			is  = require.New(t)
			g   = grpc.NewServer()
			l   = testLogger()
			err error
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mux := new(muxer)
		mux.api = testHTTPServer()
		mux.p2p = g
		mux.log = l
		mux.run = new(int32)
		mux.done = make(chan struct{})
		mux.maddr, err = multiaddr.NewMultiaddr("/ip4/1.1.1.1/tcp/2")
		is.NoError(err)

		mu := new(sync.Mutex)

		exit := atomic.NewInt32(0)
		monkey.Patch(os.Exit, func(v int) {
			exit.Store(int32(v))

			mu.Lock()
			if l, ok := mux.lis.(*syncListener); ok {
				l.Lock()
				l.Listener, _ = net.ListenTCP("tcp", nil)
				l.Unlock()
			}
			mu.Unlock()
		})

		m := &errMuxer{handleError: func(e error) bool { return true }}
		monkey.Patch(cmux.New, func(net.Listener) cmux.CMux {
			// prevent panic:
			return m
		})

		is.NotPanics(func() {
			mux.Start(ctx)
		})

		m.handleError(errors.New("test"))

		is.Equal(int32(1), exit.Load())

		mu.Lock()
		mux.lis = &syncListener{Listener: &errListener{}}
		mu.Unlock()
		*mux.run = 1

		monkey.PatchInstanceMethod(reflect.TypeOf(&http.Server{}), "Shutdown", func(*http.Server, context.Context) error {
			return errors.New("http.Shutdown error")
		})

		is.NotPanics(mux.Stop)
	})

	t.Run("should not panic when work with nil listener", func(t *testing.T) {
		var (
			is  = require.New(t)
			err error
		)

		lis := NetListener(nil)
		is.NotPanics(func() {
			is.NoError(lis.Close())
		})
		is.NotPanics(func() {
			lis.Addr()
		})
		is.NotPanics(func() {
			_, err = lis.Accept()
			is.EqualError(err, errNothingAccept.Error())
		})
	})
}
