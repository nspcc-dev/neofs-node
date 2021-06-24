package network

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddress_TLSEnabled(t *testing.T) {
	testCases := [...]struct {
		input   string
		wantTLS bool
	}{
		{"/dns4/localhost/tcp/8080", false},
		{"/dns4/localhost/tcp/8080/tls", true},
		{"/tls/dns4/localhost/tcp/8080", true},
		{"grpc://localhost:8080", false},
		{"grpcs://localhost:8080", true},
	}

	var addr Address

	for _, test := range testCases {
		err := addr.FromString(test.input)
		require.NoError(t, err)

		require.Equal(t, test.wantTLS, addr.TLSEnabled(), test.input)
	}
}
