package peerauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestCertificatePublicKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	pub, err := CertificatePublicKey(newCertificate(t, key))
	require.NoError(t, err)
	require.Equal(t, (*keys.PublicKey)(&key.PublicKey).Bytes(), pub)

	t.Run("unsupported key type", func(t *testing.T) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		_, err = CertificatePublicKey(newCertificate(t, key))
		require.EqualError(t, err, "unsupported public key type *rsa.PublicKey")
	})

	t.Run("unsupported curve", func(t *testing.T) {
		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		require.NoError(t, err)
		_, err = CertificatePublicKey(newCertificate(t, key))
		require.EqualError(t, err, "unsupported elliptic curve P-384")
	})
}

func TestPeerPublicKey(t *testing.T) {
	pub, err := PeerPublicKey(context.Background())
	require.NoError(t, err)
	require.Nil(t, pub)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := newCertificate(t, key)
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}},
	})
	pub, err = PeerPublicKey(ctx)
	require.NoError(t, err)
	require.Equal(t, (*keys.PublicKey)(&key.PublicKey).Bytes(), pub)
}

func newCertificate(t *testing.T, key crypto.Signer) *x509.Certificate {
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}
