package main

import (
	"crypto/tls"
	"fmt"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
)

func clientCertificateProvider(cfgs []grpcconfig.GRPC) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
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
			return &cert, nil
		}
	}

	return nil
}
