package peers

import (
	"context"
	"encoding"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type (
	fakeAddress struct {
		json.Marshaler
		json.Unmarshaler
		encoding.TextMarshaler
		encoding.TextUnmarshaler
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
	}

	// service is used to implement GreaterServer.
	service struct{}
)

// Hello is simple handler
func (*service) Hello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
	return &HelloResponse{
		Message: "Hello " + req.Name,
	}, nil
}

var _ multiaddr.Multiaddr = (*fakeAddress)(nil)

func (fakeAddress) Equal(multiaddr.Multiaddr) bool {
	return false
}

func (fakeAddress) Bytes() []byte {
	return nil
}

func (fakeAddress) String() string {
	return "fake"
}

func (fakeAddress) Protocols() []multiaddr.Protocol {
	return []multiaddr.Protocol{{Name: "fake"}}
}

func (fakeAddress) Encapsulate(multiaddr.Multiaddr) multiaddr.Multiaddr {
	panic("implement me")
}

func (fakeAddress) Decapsulate(multiaddr.Multiaddr) multiaddr.Multiaddr {
	panic("implement me")
}

func (fakeAddress) ValueForProtocol(code int) (string, error) {
	return "", nil
}

const testCount = 10

func newTestAddress(t *testing.T) multiaddr.Multiaddr {
	lis, err := net.Listen("tcp", "0.0.0.0:0") // nolint:gosec
	require.NoError(t, err)
	require.NoError(t, lis.Close())

	l, ok := lis.(*net.TCPListener)
	require.True(t, ok)

	_, port, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err)

	items := []string{
		"ip4",
		"127.0.0.1",
		"tcp",
		port,
	}

	maddr, err := multiaddr.NewMultiaddr("/" + strings.Join(items, "/"))
	require.NoError(t, err)

	return maddr
}

func createTestInterface(t *testing.T) Interface {
	s, err := New(Params{
		Address:   newTestAddress(t),
		Transport: transport.New(5, time.Second),
	})
	require.NoError(t, err)
	return s
}

func createTestInterfaces(t *testing.T) []Interface {
	var ifaces = make([]Interface, 0, testCount)
	for i := 0; i < testCount; i++ {
		ifaces = append(ifaces, createTestInterface(t))
	}
	return ifaces
}

func connectEachOther(t *testing.T, ifaces []Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, s := range ifaces {
		_, err := s.Listen(s.Address())
		require.NoError(t, err)

		for _, n := range ifaces {
			if s.Address().Equal(n.Address()) {
				continue // do not connect itself
			}

			_, err = n.Connect(ctx, s.Address())
			require.NoError(t, err)
		}
	}
}

func TestInterface(t *testing.T) {
	t.Run("should fail on empty address", func(t *testing.T) {
		_, err := New(Params{})
		require.EqualError(t, err, ErrEmptyAddress.Error())
	})

	t.Run("should fail on empty transport", func(t *testing.T) {
		_, err := New(Params{Address: newTestAddress(t)})
		require.EqualError(t, err, ErrEmptyTransport.Error())
	})

	t.Run("try to create multiple Interface and connect each other", func(t *testing.T) {
		ifaces := createTestInterfaces(t)
		connectEachOther(t, ifaces)
	})

	t.Run("should fail on itself connection", func(t *testing.T) {
		s := createTestInterface(t)
		_, err := s.Connect(context.Background(), s.Address())
		require.EqualError(t, err, ErrDialToSelf.Error())
	})

	t.Run("should fail when you try to remove closed connection", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		s2, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		_, err = s1.Listen(s1.Address())
		require.NoError(t, err)

		_, err = s2.Listen(s2.Address())
		require.NoError(t, err)

		con, err := s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)
		require.NoError(t, con.Close())

		err = s1.RemoveConnection(s2.Address())
		require.NoError(t, err)
	})

	t.Run("should not create connection / listener twice", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		s2, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		l1, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		l2, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		require.Equal(t, l1, l2)

		_, err = s2.Listen(s2.Address())
		require.NoError(t, err)

		c1, err := s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)

		c2, err := s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)

		require.Equal(t, c1, c2)
		require.NoError(t, c1.Close())

		err = s1.RemoveConnection(s2.Address())
		require.NoError(t, err)
	})

	t.Run("should not try to close unknown connection", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		s2, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		l1, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		l2, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		require.Equal(t, l1, l2)

		_, err = s2.Listen(s2.Address())
		require.NoError(t, err)

		_, err = s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)

		err = s1.RemoveConnection(s2.Address())
		require.NoError(t, err)

		err = s1.RemoveConnection(s2.Address())
		require.NoError(t, err)
	})

	t.Run("should shutdown without errors", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		s2, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		l1, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		l2, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		require.Equal(t, l1, l2)

		_, err = s2.Listen(s2.Address())
		require.NoError(t, err)

		_, err = s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)

		err = s1.Shutdown()
		require.NoError(t, err)

		err = s2.Shutdown()
		require.NoError(t, err)
	})

	t.Run("should fail, when shutdown with closed connections or listeners", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		s2, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		l1, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		l2, err := s1.Listen(s1.Address())
		require.NoError(t, err)

		require.Equal(t, l1, l2)

		lis, err := s2.Listen(s2.Address())
		require.NoError(t, err)

		con, err := s1.Connect(context.Background(), s2.Address())
		require.NoError(t, err)

		require.NoError(t, con.Close())

		err = s1.Shutdown()
		require.NoError(t, err)

		require.NoError(t, lis.Close())

		err = s2.Shutdown()
		require.Error(t, err)
	})

	t.Run("circuit breaker should start fail connection after N-fails", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		addr := newTestAddress(t)
		for i := 0; i < defaultAttemptsCount*2; i++ {
			_, err = s1.Connect(context.Background(), addr)
			require.Error(t, err)

			if i+1 == defaultAttemptsCount {
				_, err = s1.Listen(addr)
				require.NoError(t, err)
			}
		}
	})

	t.Run("should return error on bad multi-address", func(t *testing.T) {
		s1, err := New(Params{
			Address:   newTestAddress(t),
			Transport: transport.New(5, time.Second),
		})
		require.NoError(t, err)

		_, err = s1.Listen(&fakeAddress{})
		require.Error(t, err)
	})

	t.Run("gRPC connection test", func(t *testing.T) {
		var (
			err    error
			s1, s2 Interface
			h      = &service{}
			g      = grpc.NewServer()
			a1, a2 = newTestAddress(t), newTestAddress(t)
			tr     = transport.New(5, time.Second)
			_      = h
			done   = make(chan struct{})
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s1, err = New(Params{
			Address:   a1,
			Transport: tr,
		})
		require.NoError(t, err)

		s2, err = New(Params{
			Address:   a2,
			Transport: tr,
		})
		require.NoError(t, err)

		RegisterGreeterServer(g, h) // register service

		l, err := s1.Listen(a1)
		require.NoError(t, err)

		defer l.Close() // nolint:golint

		wg := new(sync.WaitGroup)
		wg.Add(1)

		go func() {
			close(done)

			_ = g.Serve(manet.NetListener(l))

			wg.Done()
		}()

		<-done // wait for server is start listening connections:

		// Fail connection
		con, err := s2.GRPCConnection(ctx, &fakeAddress{}, false)
		require.Nil(t, con)
		require.Error(t, err)

		con, err = s2.GRPCConnection(ctx, a1, false)
		require.NoError(t, err)

		cli := NewGreeterClient(con)
		resp, err := cli.Hello(ctx, &HelloRequest{
			Name: "Interface test",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, "Hello Interface test", resp.Message)

		g.GracefulStop()

		wg.Wait()
	})

	t.Run("test grpc connections", func(t *testing.T) {
		var (
			ifaces    = make([]Interface, 0, testCount)
			addresses = make([]multiaddr.Multiaddr, 0, testCount)
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for i := 0; i < testCount; i++ {
			addresses = append(addresses, newTestAddress(t))

			s, err := New(Params{
				Address:   addresses[i],
				Transport: transport.New(5, time.Second),
			})
			require.NoError(t, err)

			lis, err := s.Listen(addresses[i])
			require.NoError(t, err)

			svc := &service{}
			srv := grpc.NewServer()

			RegisterGreeterServer(srv, svc)

			ifaces = append(ifaces, s)

			go func() {
				l := manet.NetListener(lis)
				require.NoError(t, srv.Serve(l))
			}()
		}

		const reqName = "test"
		wg := new(sync.WaitGroup)

		for i := 0; i < testCount; i++ {
			for j := 0; j < testCount; j++ {
				wg.Add(1)
				go func(i, j int) {
					defer wg.Done()

					con, err := ifaces[i].GRPCConnection(ctx, addresses[j], false)
					require.NoError(t, err)

					cli := NewGreeterClient(con)

					resp, err := cli.Hello(ctx, &HelloRequest{Name: reqName})
					require.NoError(t, err)

					require.Equal(t, "Hello "+reqName, resp.Message)

					require.NoError(t, con.Close())
				}(i, j)

			}
		}

		wg.Wait()
	})
}
