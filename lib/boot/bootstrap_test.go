package boot

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/bootstrap"
	"github.com/stretchr/testify/require"
)

func TestBootstrapPeerParams(t *testing.T) {
	s := BootstrapPeerParams{}

	nodeInfo := &bootstrap.NodeInfo{
		Address: "address",
		PubKey:  []byte{1, 2, 3},
		Options: []string{
			"opt1",
			"opt2",
		},
	}
	s.SetNodeInfo(nodeInfo)

	require.Equal(t, nodeInfo, s.NodeInfo())
}
