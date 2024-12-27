package uriutil

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// Parse parses URI and returns a host and a flag indicating that TLS is
// enabled.
func Parse(s string) (string, bool, error) {
	uri, err := url.ParseRequestURI(s)
	if err != nil {
		if !strings.Contains(s, "/") {
			_, _, err := net.SplitHostPort(s)
			return s, false, err
		}
		return s, false, err
	}

	const (
		grpcScheme    = "grpc"
		grpcTLSScheme = "grpcs"
	)

	// check if passed string was parsed correctly
	// URIs that do not start with a slash after the scheme are interpreted as:
	// `scheme:opaque` => if `opaque` is not empty, then it is supposed that URI
	// is in `host:port` format
	if uri.Host == "" {
		uri.Host = uri.Scheme
		uri.Scheme = grpcScheme // assume GRPC by default
		if uri.Opaque != "" {
			uri.Host = net.JoinHostPort(uri.Host, uri.Opaque)
		}
	}

	switch uri.Scheme {
	case grpcTLSScheme, grpcScheme:
	default:
		return "", false, fmt.Errorf("unsupported scheme: %s", uri.Scheme)
	}

	if uri.Port() == "" {
		return "", false, errors.New("missing port in address")
	}

	return uri.Host, uri.Scheme == grpcTLSScheme, nil
}
