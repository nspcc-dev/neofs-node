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

func TestAddress_AddTLS(t *testing.T) {
	input, tls := "/dns4/localhost/tcp/8080", tls.String()

	testCases := [...]struct {
		input string
		want  string
	}{
		{input, input + tls},
		{input + tls, input + tls},
	}

	for _, test := range testCases {
		addr := Address{
			ma: buildMultiaddr(test.input, t),
		}

		addr.AddTLS()

		require.Equal(t, test.want, addr.String(), test.input)
	}
}
