package netmap

import (
	"math/big"
	"math/rand"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func Test_stackItemsToNodeInfos(t *testing.T) {
	expected := make([]netmap.NodeInfo, 4)
	for i := range expected {
		pub := make([]byte, 33)
		rand.Read(pub)

		switch i % 3 {
		case 1:
			expected[i].SetOffline()
		case 2:
			expected[i].SetOnline()
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
			state = 1
		case expected[i].IsOffline():
			state = 2
		}

		items[i] = stackitem.NewStruct([]stackitem.Item{
			stackitem.NewStruct([]stackitem.Item{
				stackitem.NewByteArray(data),
			}),
			stackitem.NewBigInteger(big.NewInt(state)),
		})
	}

	actual, err := nodeInfosFromStackItems([]stackitem.Item{stackitem.NewArray(items)}, "")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
