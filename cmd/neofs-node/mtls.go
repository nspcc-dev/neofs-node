package main

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"google.golang.org/grpc/credentials"
)

type trustedPeerCredentials struct {
	credentials.TransportCredentials
}

func trustedPeerTLSCredentials(config *tls.Config) credentials.TransportCredentials {
	return trustedPeerCredentials{TransportCredentials: credentials.NewTLS(config)}
}

func (x trustedPeerCredentials) Clone() credentials.TransportCredentials {
	return trustedPeerCredentials{TransportCredentials: x.TransportCredentials.Clone()}
}

func (x trustedPeerCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := x.TransportCredentials.ServerHandshake(conn)
	if err != nil {
		return nil, nil, err
	}

	tlsInfo, ok := authInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return conn, authInfo, nil
	}
	trustedInfo, err := peerauth.NewAuthInfo(tlsInfo)
	if err != nil {
		return conn, authInfo, nil
	}
	return conn, trustedInfo, nil
}

func clientCertificateProvider(cfgs []grpcconfig.GRPC, key *ecdsa.PrivateKey) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	for i := range cfgs {
		if !cfgs[i].TLS.Enabled {
			continue
		}

		certFile := cfgs[i].TLS.Certificate
		return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := loadTLSCertificate(certFile, key)
			if err != nil {
				return nil, fmt.Errorf("reload TLS client certificate: %w", err)
			}
			return &cert, nil
		}
	}

	return nil
}

const maxTLSCertificateFileBytes = 16 << 10 // 16 KB

// loadTLSCertificate reads a PEM-encoded certificate chain and pairs it with
// the node's private key. The leaf certificate must be issued for the node key.
func loadTLSCertificate(certFile string, key *ecdsa.PrivateKey) (tls.Certificate, error) {
	certPEM, err := readTLSCertificateFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal node TLS private key: %w", err)
	}

	return tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func readTLSCertificateFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(io.LimitReader(f, maxTLSCertificateFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxTLSCertificateFileBytes {
		return nil, fmt.Errorf("TLS certificate file exceeds %d bytes", maxTLSCertificateFileBytes)
	}
	return data, nil
}
