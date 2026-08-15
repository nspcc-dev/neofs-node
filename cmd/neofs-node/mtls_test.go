package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
)

func TestClientCertificateProvider(t *testing.T) {
	require.Nil(t, clientCertificateProvider(nil, nil))

	provider := clientCertificateProvider([]grpcconfig.GRPC{{
		TLS: grpcconfig.TLS{
			Enabled:     true,
			Certificate: "missing-certificate",
			Key:         "missing-key",
		},
	}}, nil)
	require.NotNil(t, provider)
	_, err := provider(nil)
	require.ErrorContains(t, err, "reload TLS client certificate")

	provider = clientCertificateProvider([]grpcconfig.GRPC{
		{TLS: grpcconfig.TLS{Enabled: false, Certificate: "ignored-certificate", Key: "ignored-key"}},
		{TLS: grpcconfig.TLS{Enabled: true, Certificate: "client-certificate", Key: "client-key"}},
	}, nil)
	_, err = provider(nil)
	require.ErrorContains(t, err, "client-certificate")
}

func TestTrustedPeerTLSCredentials(t *testing.T) {
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serverCert := testTLSCertificate(t, serverKey)
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
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

func TestVerifyTLSCertificatePublicKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := testTLSCertificate(t, key)
	pub, err := peerauth.CertificatePublicKey(cert)
	require.NoError(t, err)

	tlsCert := &tls.Certificate{Certificate: [][]byte{cert.Raw}}
	require.NoError(t, verifyTLSCertificatePublicKey(tlsCert, pub.Bytes()))
	require.ErrorContains(t, verifyTLSCertificatePublicKey(tlsCert, []byte("other key")), "differs from node public key")
	require.ErrorContains(t, verifyTLSCertificatePublicKey(new(tls.Certificate), pub.Bytes()), "parse TLS client certificate public key")
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
