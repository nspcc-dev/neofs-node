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
	"testing"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/stretchr/testify/require"
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
	_, _, err := new(Clients).initConnection(context.Background(), []byte("invalid"), "/dns4/example.com/tcp/443/tls")
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
