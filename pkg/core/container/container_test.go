package container

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/basic"
	"github.com/stretchr/testify/require"
)

func TestContainerMethods(t *testing.T) {
	c := new(Container)

	acl := basic.FromUint32(1)
	c.SetBasicACL(acl)
	require.True(t, basic.Equal(acl, c.BasicACL()))

	ownerID := OwnerID{1, 2, 3}
	c.SetOwnerID(ownerID)
	require.Equal(t, ownerID, c.OwnerID())

	salt := []byte{4, 5, 6}
	c.SetSalt(salt)
	require.Equal(t, salt, c.Salt())

	rule := PlacementRule{
		ReplFactor: 1,
	}
	c.SetPlacementRule(rule)
	require.Equal(t, rule, c.PlacementRule())
}
