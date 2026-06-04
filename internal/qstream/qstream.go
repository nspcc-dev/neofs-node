package qstream

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/quic-go/quic-go"
)

// ALPN is the TLS application protocol negotiated by both ends of the raw GET
// stream. It must match on the node (listener) and on every client (S3 gateway
// and node-to-node forwarder).
const ALPN = "neofs-get-quic"

const (
	StatusOK    byte = 0
	StatusError byte = 1
)

const MaxRequestSize = 64 * 1024

func Config() *quic.Config {
	return &quic.Config{
		MaxIncomingStreams: 1 << 16,
		MaxIdleTimeout:     5 * time.Minute,
		KeepAlivePeriod:    30 * time.Second,
	}
}

// ServerTLSConfig builds a server TLS config with an ephemeral self-signed
// certificate.
func ServerTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Now().AddDate(10, 0, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal key: %w", err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		return nil, fmt.Errorf("build key pair: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{ALPN},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func ClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // prototype: ephemeral self-signed server cert
		NextProtos:         []string{ALPN},
		MinVersion:         tls.VersionTLS13,
	}
}
