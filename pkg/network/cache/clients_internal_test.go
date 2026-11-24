package cache

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMultiEndpointError(t *testing.T) {
	firstErr := errors.New("test err")

	err := newMultiEndpointError("NODE_X", firstErr)
	require.EqualError(t, err, "all NODE_X endpoints failed, first error: test err")
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, clientcore.ErrAllConnectionsSkipped)
}

func TestNodeTLSConfig(t *testing.T) {
	expectedKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	t.Run("matching self-signed certificate", func(t *testing.T) {
		cfg := newNodeTLSConfig(&expectedKey.PublicKey, nil)
		require.True(t, cfg.InsecureSkipVerify)
		require.Empty(t, cfg.ServerName)
		require.NoError(t, cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{newSelfSignedCertificate(t, expectedKey)},
		}))
	})

	t.Run("client certificate", func(t *testing.T) {
		expected := new(tls.Certificate)
		cfg := newNodeTLSConfig(&expectedKey.PublicKey, func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return expected, nil
		})
		require.Empty(t, cfg.ServerName)
		actual, err := cfg.GetClientCertificate(nil)
		require.NoError(t, err)
		require.Same(t, expected, actual)
	})

	t.Run("wrong key", func(t *testing.T) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		err = newNodeTLSConfig(&expectedKey.PublicKey, nil).VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{newSelfSignedCertificate(t, key)},
		})
		require.ErrorIs(t, err, clientcore.ErrWrongPublicKey)
	})

	t.Run("no certificate", func(t *testing.T) {
		err := newNodeTLSConfig(&expectedKey.PublicKey, nil).VerifyConnection(tls.ConnectionState{})
		require.EqualError(t, err, "server did not provide TLS certificate")
	})

	t.Run("unsupported key type", func(t *testing.T) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		err = newNodeTLSConfig(&expectedKey.PublicKey, nil).VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{newSelfSignedCertificate(t, key)},
		})
		require.EqualError(t, err, "server TLS certificate has unsupported public key type *rsa.PublicKey")
	})

	t.Run("unsupported elliptic curve", func(t *testing.T) {
		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		require.NoError(t, err)

		err = newNodeTLSConfig(&expectedKey.PublicKey, nil).VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{newSelfSignedCertificate(t, key)},
		})
		require.EqualError(t, err, "server TLS certificate has unsupported elliptic curve P-384")
	})
}

func TestInvalidTLSPublicKey(t *testing.T) {
	_, _, err := new(Clients).initConnection(context.Background(), []byte("invalid"), "grpcs://example.com:443")
	require.ErrorContains(t, err, "parse node public key")
}

func TestConnectionsIsMutuallyAuthenticated(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		hasClientCertificate bool
		endpoints            []string
		peerRequestedCert    bool
		expected             bool
	}{
		{name: "single TLS endpoint", hasClientCertificate: true, endpoints: []string{"/dns4/example.com/tcp/443/tls"}, peerRequestedCert: true, expected: true},
		{name: "multiple TLS endpoints", hasClientCertificate: true, endpoints: []string{"/dns4/one.example.com/tcp/443/tls", "/dns4/two.example.com/tcp/443/tls"}, peerRequestedCert: true, expected: true},
		{name: "server did not request certificate", hasClientCertificate: true, endpoints: []string{"/dns4/example.com/tcp/443/tls"}},
		{name: "no client certificate", endpoints: []string{"/dns4/example.com/tcp/443/tls"}},
		{name: "plain endpoint", hasClientCertificate: true, endpoints: []string{"/dns4/example.com/tcp/8080"}},
		{name: "mixed endpoints", hasClientCertificate: true, endpoints: []string{"/dns4/example.com/tcp/443/tls", "/dns4/example.com/tcp/8080"}},
		{name: "empty endpoints", hasClientCertificate: true},
		{name: "invalid endpoint", hasClientCertificate: true, endpoints: []string{"invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conns := &connections{
				hasClientCertificate: tc.hasClientCertificate,
				m:                    make(map[string]*endpointClient, len(tc.endpoints)),
			}
			for i := range tc.endpoints {
				conns.m[tc.endpoints[i]] = &endpointClient{withTLS: tc.endpoints[i] != "/dns4/example.com/tcp/8080"}
				conns.m[tc.endpoints[i]].mtls.Store(tc.peerRequestedCert)
			}
			conns.updateMutualAuthenticationStatus()
			require.Equal(t, tc.expected, conns.IsMutuallyAuthenticated())
		})
	}
}

func newSelfSignedCertificate(t *testing.T, key crypto.Signer) *x509.Certificate {
	now := time.Now()
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, key.Public(), key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

type blackholeServer struct {
	addr     string
	accepted atomic.Int64
	notify   chan struct{}
}

func newBlackholeServer(t testing.TB) *blackholeServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &blackholeServer{
		addr:   ln.Addr().String(),
		notify: make(chan struct{}, 32),
	}

	var (
		mtx   sync.Mutex
		conns []net.Conn
		wg    sync.WaitGroup
	)
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			srv.accepted.Add(1)
			select {
			case srv.notify <- struct{}{}:
			default:
			}
			mtx.Lock()
			conns = append(conns, conn)
			mtx.Unlock()
		}
	})

	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
		mtx.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mtx.Unlock()
	})

	return srv
}

func (s *blackholeServer) waitAccepted(t testing.TB) {
	t.Helper()
	select {
	case <-s.notify:
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for dial to reach listener")
	}
}

func newTestClients() *Clients {
	return NewClients(zap.NewNop(), nil, 5*time.Second, time.Second, 10*time.Second, 5*time.Second, nil, nil)
}

func newTestNodeInfo(pub []byte, endpoint string) netmap.NodeInfo {
	var ni netmap.NodeInfo
	ni.SetPublicKey(pub)
	ni.SetNetworkEndpoints(endpoint)
	return ni
}

func getWithTimeout(clients *Clients, info netmap.NodeInfo, d time.Duration) (clientcore.MultiAddressClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return clients.Get(ctx, info)
}

func startGet(ctx context.Context, clients *Clients, info netmap.NodeInfo) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := clients.Get(ctx, info)
		done <- err
	}()
	return done
}

func TestInitBackoff(t *testing.T) {
	require.Equal(t, time.Duration(0), initBackoff(0))
	require.Equal(t, time.Second, initBackoff(1))
	require.Equal(t, 1600*time.Millisecond, initBackoff(2))
	require.Equal(t, 2560*time.Millisecond, initBackoff(3))
	require.Less(t, initBackoff(11), 120*time.Second)
	require.Equal(t, 120*time.Second, initBackoff(12))
	require.Equal(t, 120*time.Second, initBackoff(1000))
}

func TestClientsGetBackoff(t *testing.T) {
	t.Run("backoff avoids redialing within window and retries after", func(t *testing.T) {
		srv := newBlackholeServer(t)
		clients := newTestClients()
		var (
			clockMtx sync.Mutex
			now      = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		)
		clients.now = func() time.Time {
			clockMtx.Lock()
			defer clockMtx.Unlock()
			return now
		}
		advance := func(d time.Duration) {
			clockMtx.Lock()
			now = now.Add(d)
			clockMtx.Unlock()
		}

		dead := newTestNodeInfo([]byte("dead-node-1"), srv.addr)

		_, err1 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err1)
		require.Equal(t, int64(1), srv.accepted.Load())

		start := time.Now()
		_, err2 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Less(t, time.Since(start), 50*time.Millisecond)
		require.ErrorIs(t, err2, err1)
		require.ErrorContains(t, err2, "recently failed")
		require.Equal(t, int64(1), srv.accepted.Load())

		advance(999 * time.Millisecond)
		_, err3 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.ErrorIs(t, err3, err1)
		require.Equal(t, int64(1), srv.accepted.Load())

		advance(time.Millisecond)
		_, err4 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err4)
		require.NotContains(t, err4.Error(), "recently failed")
		require.Equal(t, int64(2), srv.accepted.Load())

		advance(1599 * time.Millisecond)
		_, err5 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.ErrorIs(t, err5, err4)
		require.Equal(t, int64(2), srv.accepted.Load())

		advance(time.Millisecond)
		_, err6 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err6)
		require.NotContains(t, err6.Error(), "recently failed")
		require.Equal(t, int64(3), srv.accepted.Load())
	})

	t.Run("concurrent gets deduplicate dial", func(t *testing.T) {
		srv := newBlackholeServer(t)
		clients := newTestClients()
		dead := newTestNodeInfo([]byte("dead-node-2"), srv.addr)

		const waiters = 4
		errs := make([]error, waiters+1)

		var wg sync.WaitGroup
		wg.Go(func() {
			_, errs[0] = getWithTimeout(clients, dead, 300*time.Millisecond)
		})
		srv.waitAccepted(t)

		for i := range waiters {
			wg.Go(func() {
				_, errs[i+1] = getWithTimeout(clients, dead, time.Second)
			})
		}
		wg.Wait()

		for i := range errs {
			require.Error(t, errs[i])
		}
		require.Equal(t, int64(1), srv.accepted.Load())
	})

	t.Run("canceled context does not trigger backoff", func(t *testing.T) {
		srv := newBlackholeServer(t)
		clients := newTestClients()
		dead := newTestNodeInfo([]byte("dead-node-cancel"), srv.addr)

		preCanceled, cancelPre := context.WithCancel(context.Background())
		cancelPre()
		_, err := clients.Get(preCanceled, dead)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int64(0), srv.accepted.Load())

		dialCtx, cancelDial := context.WithCancel(context.Background())
		dialDone := startGet(dialCtx, clients, dead)
		srv.waitAccepted(t)

		cancelDial()
		require.Error(t, <-dialDone)

		_, err2 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err2)
		require.NotContains(t, err2.Error(), "recently failed")
		require.Equal(t, int64(2), srv.accepted.Load())
	})

	t.Run("expired context while waiting for lock does not trigger backoff", func(t *testing.T) {
		srv1 := newBlackholeServer(t)
		srv2 := newBlackholeServer(t)
		clients := newTestClients()
		node1 := newTestNodeInfo([]byte("dead-node-lock-1"), srv1.addr)
		node2 := newTestNodeInfo([]byte("dead-node-lock-2"), srv2.addr)

		dial1Ctx, cancel1 := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel1()
		dial1Done := startGet(dial1Ctx, clients, node1)
		srv1.waitAccepted(t)

		dial2Ctx, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel2()
		dial2Done := startGet(dial2Ctx, clients, node2)

		require.Error(t, <-dial1Done)
		require.ErrorIs(t, <-dial2Done, context.DeadlineExceeded)
		require.Equal(t, int64(0), srv2.accepted.Load())

		_, err := getWithTimeout(clients, node2, 150*time.Millisecond)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "recently failed")
		require.Equal(t, int64(1), srv2.accepted.Load())
	})

	t.Run("SyncWithNewNetmap clears failure state for removed node", func(t *testing.T) {
		srv := newBlackholeServer(t)
		clients := newTestClients()
		dead := newTestNodeInfo([]byte("dead-node-4"), srv.addr)
		other := newTestNodeInfo([]byte("other-node"), "127.0.0.1:1")

		_, err1 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err1)
		require.Equal(t, int64(1), srv.accepted.Load())

		clients.SyncWithNewNetmap(context.Background(), []netmap.NodeInfo{dead, other}, -1)
		_, err2 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.ErrorIs(t, err2, err1)
		require.ErrorContains(t, err2, "recently failed")
		require.Equal(t, int64(1), srv.accepted.Load())

		clients.SyncWithNewNetmap(context.Background(), []netmap.NodeInfo{other}, -1)
		_, err3 := getWithTimeout(clients, dead, 150*time.Millisecond)
		require.Error(t, err3)
		require.NotContains(t, err3.Error(), "recently failed")
		require.Equal(t, int64(2), srv.accepted.Load())
	})
}
