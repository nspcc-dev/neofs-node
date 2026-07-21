package peerauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// CertificatePublicKey returns the P-256 public key from cert in the
// compressed format.
func CertificatePublicKey(cert *x509.Certificate) ([]byte, error) {
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("unsupported public key type %T", cert.PublicKey)
	}
	if pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("unsupported elliptic curve %s", pub.Curve.Params().Name)
	}
	return (*keys.PublicKey)(pub).Bytes(), nil
}

// PeerPublicKey returns the public key authenticated by the TLS connection.
// It returns nil when the request has no TLS client certificate.
func PeerPublicKey(ctx context.Context) ([]byte, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, nil
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, nil
	}
	if len(info.State.PeerCertificates) == 0 {
		return nil, nil
	}
	key, err := CertificatePublicKey(info.State.PeerCertificates[0])
	if err != nil {
		return nil, fmt.Errorf("invalid TLS peer certificate: %w", err)
	}
	return key, nil
}
