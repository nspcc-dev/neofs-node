package availability

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/stretchr/testify/require"
)

func TestNodeTLSConfig(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := testCertificate(t, key)
	expectedKey, err := peerauth.CertificatePublicKey(cert)
	require.NoError(t, err)

	t.Run("matching certificate", func(t *testing.T) {
		err := nodeTLSConfig(expectedKey.Bytes()).VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}})
		require.NoError(t, err)
	})

	t.Run("wrong certificate", func(t *testing.T) {
		otherKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		err = nodeTLSConfig(expectedKey.Bytes()).VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{testCertificate(t, otherKey)}})
		require.EqualError(t, err, "server TLS certificate public key mismatches network map candidate")
	})

	t.Run("missing certificate", func(t *testing.T) {
		err := nodeTLSConfig(expectedKey.Bytes()).VerifyConnection(tls.ConnectionState{})
		require.EqualError(t, err, "server did not provide TLS certificate")
	})
}

func testCertificate(t *testing.T, key *ecdsa.PrivateKey) *x509.Certificate {
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Minute),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}
