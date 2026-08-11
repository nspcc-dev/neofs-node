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

// AuthInfo is TLS authentication information for a peer with a supported
// public key. It is set once during the TLS handshake.
type AuthInfo struct {
	credentials.TLSInfo
	PublicKey *keys.PublicKey
}

// AuthType implements [credentials.AuthInfo].
func (AuthInfo) AuthType() string { return "tls" }

// NewAuthInfo returns authentication information for a TLS peer certificate.
func NewAuthInfo(info credentials.TLSInfo) (AuthInfo, error) {
	if len(info.State.PeerCertificates) == 0 {
		return AuthInfo{}, fmt.Errorf("missing TLS peer certificate")
	}
	key, err := CertificatePublicKey(info.State.PeerCertificates[0])
	if err != nil {
		return AuthInfo{}, err
	}
	return AuthInfo{TLSInfo: info, PublicKey: key}, nil
}

// IsTrustedPeer reports whether ctx was authenticated during the TLS handshake.
func IsTrustedPeer(ctx context.Context) bool {
	_, ok := authenticatedPeerInfo(ctx)
	return ok
}

func authenticatedPeerInfo(ctx context.Context) (AuthInfo, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return AuthInfo{}, false
	}
	info, ok := p.AuthInfo.(AuthInfo)
	return info, ok
}

// CertificatePublicKey returns the P-256 public key from cert.
func CertificatePublicKey(cert *x509.Certificate) (*keys.PublicKey, error) {
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("unsupported public key type %T", cert.PublicKey)
	}
	if pub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("unsupported elliptic curve %s", pub.Curve.Params().Name)
	}
	return (*keys.PublicKey)(pub), nil
}

// CertificatePublicKeyFromRaw returns the P-256 public key from the first TLS
// certificate in a handshake chain.
func CertificatePublicKeyFromRaw(rawCerts [][]byte) ([]byte, error) {
	if len(rawCerts) == 0 {
		return nil, fmt.Errorf("missing TLS peer certificate")
	}
	cert, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return nil, fmt.Errorf("parse TLS peer certificate: %w", err)
	}
	pub, err := CertificatePublicKey(cert)
	if err != nil {
		return nil, err
	}
	return pub.Bytes(), nil
}

// PeerPublicKey returns the public key authenticated by the TLS connection.
// It returns nil when the request has no TLS client certificate.
func PeerPublicKey(ctx context.Context) (*keys.PublicKey, error) {
	info, ok := authenticatedPeerInfo(ctx)
	if !ok {
		return nil, nil
	}
	return info.PublicKey, nil
}
