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
	}

	for _, test := range testCases {
		addr := Address{
			ma: buildMultiaddr(test.input, t),
		}

		require.Equal(t, test.wantTLS, addr.TLSEnabled(), test.input)
	}
}
