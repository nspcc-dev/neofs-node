package peerauth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

// TLSServerName is the TLS SNI a storage node sends when dialing a peer over the
// shared public port. The server uses it to distinguish inter-node mTLS
// connections from regular client TLS connections.
const TLSServerName = "neofs.internode.mtls"

// ClientTLSServerName is the SNI a client sends to opt into client-side mTLS,
// telling it apart from inter-node ([TLSServerName]) and plain TLS connections.
const ClientTLSServerName = "neofs.client.mtls"

// GenerateSelfSignedCert builds a self-signed TLS certificate that uses the
// node's identity ECDSA key as both subject key and signing key.
func GenerateSelfSignedCert(priv *ecdsa.PrivateKey) (tls.Certificate, error) {
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create x509 certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse generated certificate: %w", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        leaf,
	}, nil
}

// CompressedPubKey returns the compressed-form public key bytes of an ECDSA
// certificate, matching the format used in NeoFS network maps.
func CompressedPubKey(cert *x509.Certificate) ([]byte, error) {
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("peer certificate public key is not ECDSA")
	}
	return (*keys.PublicKey)(pub).Bytes(), nil
}

// PeerPubKey parses the first certificate from a TLS handshake's raw chain and
// returns the peer's compressed public key bytes.
func PeerPubKey(rawCerts [][]byte) ([]byte, error) {
	if len(rawCerts) == 0 {
		return nil, errors.New("no peer certificate")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return nil, fmt.Errorf("parse peer certificate: %w", err)
	}
	return CompressedPubKey(cert)
}
