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

		expected[i].SetState(netmap.NodeState(i % 3))
		expected[i].SetPublicKey(pub)

		var attr netmap.NodeAttribute
		attr.SetKey("key")
		attr.SetValue(strconv.Itoa(i))
		expected[i].SetAttributes(attr)
	}

	items := make([]stackitem.Item, 4)
	for i := range items {
		state := int64(expected[i].State())
		if state != 0 { // In contract online=1, offline=2, in API it is the other way.
			state = 3 - state
		}

		items[i] = stackitem.NewStruct([]stackitem.Item{
			stackitem.NewStruct([]stackitem.Item{
				stackitem.NewByteArray(expected[i].Marshal()),
			}),
			stackitem.NewBigInteger(big.NewInt(state)),
		})
	}

	actual, err := nodeInfosFromStackItems([]stackitem.Item{stackitem.NewArray(items)}, "")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
