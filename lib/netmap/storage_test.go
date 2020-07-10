package netmap

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/stretchr/testify/require"
)

func TestGetResult(t *testing.T) {
	s := GetResult{}

	nm := NewNetmap()
	require.NoError(t,
		nm.AddNode(&bootstrap.NodeInfo{
			Address: "address",
			PubKey:  []byte{1, 2, 3},
		}),
	)
	s.SetNetMap(nm)

	require.Equal(t, nm, s.NetMap())
}
