package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
)

func TestClientCertificateProvider(t *testing.T) {
	key := newTLSKey(t)
	require.Nil(t, clientCertificateProvider(nil, key))

	provider := clientCertificateProvider([]grpcconfig.GRPC{{
		TLS: grpcconfig.TLS{
			Enabled:     true,
			Certificate: "missing-certificate",
			Key:         "ignored-key",
		},
	}}, key)
	require.NotNil(t, provider)
	_, err := provider(nil)
	require.ErrorContains(t, err, "reload TLS client certificate")

	provider = clientCertificateProvider([]grpcconfig.GRPC{
		{TLS: grpcconfig.TLS{Enabled: false, Certificate: "ignored-certificate", Key: "ignored-key"}},
		{TLS: grpcconfig.TLS{Enabled: true, Certificate: "client-certificate", Key: "ignored-key"}},
	}, key)
	_, err = provider(nil)
	require.ErrorContains(t, err, "client-certificate")

	certFile := writeTLSCertificate(t, testCertificate(t, key))
	provider = clientCertificateProvider([]grpcconfig.GRPC{{
		TLS: grpcconfig.TLS{Enabled: true, Certificate: certFile},
	}}, key)
	cert, err := provider(nil)
	require.NoError(t, err)
	require.Equal(t, key, cert.PrivateKey)
}

func TestLoadTLSCertificate(t *testing.T) {
	key := newTLSKey(t)

	t.Run("certificate chain", func(t *testing.T) {
		caKey := newTLSKey(t)
		ca := testCertificate(t, caKey)
		leaf := testCertificateSignedBy(t, key, ca, caKey)

		cert, err := loadTLSCertificate(writeTLSCertificate(t, leaf, ca), key)
		require.NoError(t, err)
		require.Len(t, cert.Certificate, 2)
		require.Equal(t, key, cert.PrivateKey)
	})

	t.Run("different node key", func(t *testing.T) {
		cert, err := loadTLSCertificate(writeTLSCertificate(t, testCertificate(t, newTLSKey(t))), key)
		require.ErrorContains(t, err, "private key does not match public key")
		require.Empty(t, cert)
	})

	t.Run("invalid certificate", func(t *testing.T) {
		certFile := filepath.Join(t.TempDir(), "cert.pem")
		require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("invalid")}), 0o600))
		cert, err := loadTLSCertificate(certFile, key)
		require.ErrorContains(t, err, "malformed certificate")
		require.Empty(t, cert)
	})

	t.Run("no certificate", func(t *testing.T) {
		certFile := filepath.Join(t.TempDir(), "cert.pem")
		require.NoError(t, os.WriteFile(certFile, []byte("not a PEM certificate"), 0o600))
		cert, err := loadTLSCertificate(certFile, key)
		require.EqualError(t, err, "tls: failed to find any PEM data in certificate input")
		require.Empty(t, cert)
	})

	t.Run("too large", func(t *testing.T) {
		certFile := filepath.Join(t.TempDir(), "cert.pem")
		require.NoError(t, os.WriteFile(certFile, make([]byte, maxTLSCertificateFileBytes+1), 0o600))
		cert, err := loadTLSCertificate(certFile, key)
		require.EqualError(t, err, "TLS certificate file exceeds 16384 bytes")
		require.Empty(t, cert)
	})
}

func newTLSKey(t *testing.T) *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

func testCertificate(t *testing.T, key crypto.Signer) *x509.Certificate {
	return testCertificateSignedBy(t, key, nil, key)
}

func testCertificateSignedBy(t *testing.T, key crypto.Signer, parent *x509.Certificate, signer crypto.Signer) *x509.Certificate {
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Minute),
	}
	if parent == nil {
		parent = tmpl
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, key.Public(), signer)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}

func writeTLSCertificate(t *testing.T, certs ...*x509.Certificate) string {
	var data []byte
	for _, cert := range certs {
		data = append(data, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})...)
	}
	path := filepath.Join(t.TempDir(), "cert.pem")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func TestTrustedPeerTLSCredentials(t *testing.T) {
	serverKey := newTLSKey(t)
	serverCert := testTLSCertificate(t, serverKey)
	clientKey := newTLSKey(t)
	clientCert := testTLSCertificate(t, clientKey)

	serverConn, clientConn := net.Pipe()
	serverCreds := trustedPeerTLSCredentials(&tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}},
		ClientAuth:   tls.RequestClientCert,
		NextProtos:   []string{"h2"},
	})
	serverResult := make(chan credentials.AuthInfo, 1)
	serverErr := make(chan error, 1)
	go func() {
		_, info, err := serverCreds.ServerHandshake(serverConn)
		if err == nil {
			serverResult <- info
		}
		serverErr <- err
	}()

	clientTLSConn := tls.Client(clientConn, &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{{Certificate: [][]byte{clientCert.Raw}, PrivateKey: clientKey}},
		NextProtos:         []string{"h2"},
	})
	require.NoError(t, clientTLSConn.Handshake())
	require.NoError(t, <-serverErr)
	_ = clientConn.Close()
	info := <-serverResult
	authInfo, ok := info.(peerauth.AuthInfo)
	require.True(t, ok)
	require.Equal(t, (*keys.PublicKey)(&clientKey.PublicKey), authInfo.PublicKey)
}

func testTLSCertificate(t *testing.T, key *ecdsa.PrivateKey) *x509.Certificate {
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}
