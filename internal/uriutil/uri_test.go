package uriutil_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/uriutil"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	for _, tc := range []struct {
		s       string
		host    string
		withTLS bool
	}{
		// no scheme (TCP)
		{s: "127.0.0.1:8080", host: "127.0.0.1:8080", withTLS: false},
		{s: "st1.storage.fs.neo.org:8080", host: "st1.storage.fs.neo.org:8080", withTLS: false},
		// with scheme and port
		{s: "grpc://127.0.0.1:8080", host: "127.0.0.1:8080", withTLS: false},
		{s: "grpc://st1.storage.fs.neo.org:8080", host: "st1.storage.fs.neo.org:8080", withTLS: false},
		{s: "grpcs://127.0.0.1:8082", host: "127.0.0.1:8082", withTLS: true},
		{s: "grpcs://st1.storage.fs.neo.org:8082", host: "st1.storage.fs.neo.org:8082", withTLS: true},
	} {
		host, withTLS, err := uriutil.Parse(tc.s)
		require.NoError(t, err, tc.s)
		require.Equal(t, tc.host, host, tc.s)
		require.Equal(t, tc.withTLS, withTLS, tc.s)
	}

	t.Run("invalid", func(t *testing.T) {
		for _, tc := range []struct {
			name, s, err string
		}{
			{name: "unsupported scheme", s: "unknown://st1.storage.fs.neo.org:8082", err: "unsupported scheme: unknown"},
			{name: "garbage URI", s: "not a URI", err: "address not a URI: missing port in address"},
			{name: "port only", s: "8080", err: "address 8080: missing port in address"},
			{name: "ip only", s: "127.0.0.1", err: "address 127.0.0.1: missing port in address"},
			{name: "host only", s: "st1.storage.fs.neo.org", err: "address st1.storage.fs.neo.org: missing port in address"},
			{name: "multiaddr", s: "/ip4/127.0.0.1/tcp/8080", err: "missing port in address"},
			{name: "ip with scheme without port", s: "grpc://127.0.0.1", err: "missing port in address"},
			{name: "host with scheme without port", s: "grpc://st1.storage.fs.neo.org", err: "missing port in address"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := uriutil.Parse(tc.s)
				require.EqualError(t, err, tc.err)
			})
		}
	})
}
