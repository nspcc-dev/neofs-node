package peers

import (
	"context"
	"encoding"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/network"
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

func TestInterface(t *testing.T) {
	t.Run("gRPC connection test", func(t *testing.T) {
		var (
			err  error
			s    Interface
			h    = &service{}
			g    = grpc.NewServer()
			a1   = newTestAddress(t)
			_    = h
			done = make(chan struct{})
		)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s, err = New(Params{})
		require.NoError(t, err)

		RegisterGreeterServer(g, h) // register service

		l, err := network.Listen(a1)
		require.NoError(t, err)

		defer l.Close() // nolint:golint

		wg := new(sync.WaitGroup)
		wg.Add(1)

		go func() {
			close(done)

			_ = g.Serve(l)

			wg.Done()
		}()

		<-done // wait for server is start listening connections:

		// Fail connection
		con, err := s.GRPCConnection(ctx, &fakeAddress{})
		require.Nil(t, con)
		require.Error(t, err)

		con, err = s.GRPCConnection(ctx, a1)
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

			s, err := New(Params{})
			require.NoError(t, err)

			lis, err := network.Listen(addresses[i])
			require.NoError(t, err)

			svc := &service{}
			srv := grpc.NewServer()

			RegisterGreeterServer(srv, svc)

			ifaces = append(ifaces, s)

			go func() {
				require.NoError(t, srv.Serve(lis))
			}()
		}

		const reqName = "test"
		wg := new(sync.WaitGroup)

		for i := 0; i < testCount; i++ {
			for j := 0; j < testCount; j++ {
				wg.Add(1)
				go func(i, j int) {
					defer wg.Done()

					con, err := ifaces[i].GRPCConnection(ctx, addresses[j])
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
