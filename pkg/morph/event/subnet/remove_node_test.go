package subnetevents_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	. "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/stretchr/testify/require"
)

func TestParseRemoveNode(t *testing.T) {
	t.Run("wrong number of arguments", func(t *testing.T) {
		_, err := ParseRemoveNode(createNotifyEventFromItems([]stackitem.Item{}))
		require.Error(t, err)
	})

	t.Run("invalid item type", func(t *testing.T) {
		args := []stackitem.Item{stackitem.NewMap(), stackitem.Make(123)}
		_, err := ParseRemoveNode(createNotifyEventFromItems(args))
		require.Error(t, err)
	})

	subnetID := subnetid.ID{}
	subnetID.SetNumber(123)

	rawSubnetID, err := subnetID.Marshal()
	require.NoError(t, err)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)

	pub := priv.PublicKey()

	t.Run("good", func(t *testing.T) {
		args := []stackitem.Item{stackitem.NewByteArray(rawSubnetID), stackitem.Make(pub.Bytes())}

		e, err := ParseRemoveNode(createNotifyEventFromItems(args))
		require.NoError(t, err)

		gotRaw := e.(RemoveNode).SubnetworkID()
		require.NoError(t, err)

		require.Equal(t, rawSubnetID, gotRaw)
		require.Equal(t, pub.Bytes(), e.(RemoveNode).Node())
	})
}

func createNotifyEventFromItems(items []stackitem.Item) *subscriptions.NotificationEvent {
	return &subscriptions.NotificationEvent{
		NotificationEvent: state.NotificationEvent{
			Item: stackitem.NewArray(items),
		},
	}
}
