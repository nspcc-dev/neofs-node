package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

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

func clientCertificateProvider(cfgs []grpcconfig.GRPC, expectedPublicKey []byte) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	for i := range cfgs {
		if !cfgs[i].TLS.Enabled {
			continue
		}

		certFile, keyFile := cfgs[i].TLS.Certificate, cfgs[i].TLS.Key
		return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("reload TLS client certificate: %w", err)
			}
			if err := verifyTLSCertificatePublicKey(&cert, expectedPublicKey); err != nil {
				return nil, err
			}
			return &cert, nil
		}
	}

	return nil
}

func verifyTLSCertificatePublicKey(cert *tls.Certificate, expected []byte) error {
	pub, err := peerauth.CertificatePublicKeyFromRaw(cert.Certificate)
	if err != nil {
		return fmt.Errorf("parse TLS client certificate public key: %w", err)
	}
	if !bytes.Equal(pub, expected) {
		return errors.New("TLS client certificate public key differs from node public key")
	}
	return nil
}
