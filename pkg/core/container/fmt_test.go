package container

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestCheckFormat(t *testing.T) {
	c := container.New()

	require.Error(t, CheckFormat(c))

	policy := netmap.NewPlacementPolicy()
	c.SetPlacementPolicy(policy)

	require.Error(t, CheckFormat(c))

	c.SetVersion(version.Current())

	require.Error(t, CheckFormat(c))

	oid := owner.NewIDFromPublicKey(&test.DecodeKey(-1).PublicKey)

	c.SetOwnerID(oid)

	// set incorrect nonce
	cV2 := c.ToV2()
	cV2.SetNonce([]byte{1, 2, 3})
	c = container.NewContainerFromV2(cV2)

	require.Error(t, CheckFormat(c))

	c.SetNonceUUID(uuid.New())

	require.NoError(t, CheckFormat(c))

	// set empty value attribute
	attr1 := container.NewAttribute()
	attr1.SetKey("attr")
	attrs := container.Attributes{attr1}

	c.SetAttributes(attrs)

	require.ErrorIs(t, CheckFormat(c), errEmptyAttribute)

	// add same key attribute
	attr2 := container.NewAttribute()
	attr2.SetKey(attr1.Key())
	attr2.SetValue("val")

	attr1.SetValue(attr2.Value())

	attrs = append(attrs, attr2)

	c.SetAttributes(attrs)

	require.ErrorIs(t, CheckFormat(c), errRepeatedAttributes)

	attr2.SetKey(attr1.Key() + "smth")

	require.NoError(t, CheckFormat(c))
}
