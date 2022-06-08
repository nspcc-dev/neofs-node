package container

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	netmaptest "github.com/nspcc-dev/neofs-sdk-go/netmap/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestCheckFormat(t *testing.T) {
	c := container.New()

	require.Error(t, CheckFormat(c))

	policy := netmaptest.PlacementPolicy()
	c.SetPlacementPolicy(&policy)

	require.Error(t, CheckFormat(c))

	ver := version.Current()
	c.SetVersion(&ver)

	require.Error(t, CheckFormat(c))

	var idUser user.ID
	user.IDFromKey(&idUser, test.DecodeKey(-1).PublicKey)

	c.SetOwnerID(&idUser)

	// set incorrect nonce
	cV2 := c.ToV2()
	cV2.SetNonce([]byte{1, 2, 3})
	c = container.NewContainerFromV2(cV2)

	require.Error(t, CheckFormat(c))

	c.SetNonceUUID(uuid.New())

	require.NoError(t, CheckFormat(c))

	// set empty value attribute
	var attr1 container.Attribute
	attr1.SetKey("attr")
	attrs := container.Attributes{attr1}

	c.SetAttributes(attrs)

	require.ErrorIs(t, CheckFormat(c), errEmptyAttribute)

	// add same key attribute
	var attr2 container.Attribute
	attr2.SetKey(attr1.Key())
	attr2.SetValue("val")

	attrs[0].SetValue(attr2.Value())

	attrs = append(attrs, attr2)

	c.SetAttributes(attrs)

	require.ErrorIs(t, CheckFormat(c), errRepeatedAttributes)

	attrs[1].SetKey(attr1.Key() + "smth")

	c.SetAttributes(attrs)

	require.NoError(t, CheckFormat(c))
}
