package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"slices"
	"time"

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

func usesSelfSignedTLS(cfgs []grpcconfig.GRPC) bool {
	for i := range cfgs {
		if isSelfSignedTLS(cfgs[i].TLS) {
			return true
		}
	}
	return false
}

func isSelfSignedTLS(cfg grpcconfig.TLS) bool {
	return cfg.Enabled && cfg.Certificate == ""
}

func clientCertificateProvider(cfgs []grpcconfig.GRPC, key *ecdsa.PrivateKey, cert *tls.Certificate) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if cert != nil {
		return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return cert, nil
		}
	}

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

func selfSignedTLSCertificate(key *ecdsa.PrivateKey, cfgs []grpcconfig.GRPC) (*tls.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}

	now := time.Now()
	dnsNames, ipAddresses := selfSignedTLSNames(cfgs)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "NeoFS node",
		},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		DNSNames:              dnsNames,
		IPAddresses:           ipAddresses,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	return &tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, nil
}

func selfSignedTLSNames(cfgs []grpcconfig.GRPC) ([]string, []net.IP) {
	var dnsNames []string
	var ipAddresses []net.IP
	for i := range cfgs {
		if !isSelfSignedTLS(cfgs[i].TLS) {
			continue
		}
		host, _, err := net.SplitHostPort(cfgs[i].Endpoint)
		if err != nil || host == "" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			if !slices.ContainsFunc(ipAddresses, ip.Equal) {
				ipAddresses = append(ipAddresses, ip)
			}
			continue
		}
		if !slices.Contains(dnsNames, host) {
			dnsNames = append(dnsNames, host)
		}
	}
	return dnsNames, ipAddresses
}
