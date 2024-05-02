package netmap

import (
	"math/big"
	"math/rand"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	netmapcontract "github.com/nspcc-dev/neofs-contract/contracts/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func Test_stackItemsToNodeInfos(t *testing.T) {
	expected := make([]netmap.NodeInfo, 4)
	for i := range expected {
		pub := make([]byte, 33)
		//nolint:staticcheck
		rand.Read(pub)

		switch i % 3 {
		default:
			expected[i].SetOffline()
		case int(netmapcontract.NodeStateOnline):
			expected[i].SetOnline()
		case int(netmapcontract.NodeStateMaintenance):
			expected[i].SetMaintenance()
		}

		expected[i].SetPublicKey(pub)

		expected[i].SetAttribute("key", strconv.Itoa(i))
	}

	items := make([]stackitem.Item, 4)
	for i := range items {
		data := expected[i].Marshal()

		var state int64

		switch {
		case expected[i].IsOnline():
			state = int64(netmapcontract.NodeStateOnline)
		case expected[i].IsOffline():
			state = int64(netmapcontract.NodeStateOffline)
		case expected[i].IsMaintenance():
			state = int64(netmapcontract.NodeStateMaintenance)
		}

		items[i] = stackitem.NewStruct([]stackitem.Item{
			stackitem.NewByteArray(data),
			stackitem.NewBigInteger(big.NewInt(state)),
		})
	}

	actual, err := decodeNodeList(stackitem.NewArray(items))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
