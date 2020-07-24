package headers

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestSupportedType(t *testing.T) {
	for _, typ := range []Type{
		TypeLink,
		TypeUser,
		TypeTransform,
		TypeTombstone,
		TypeSessionToken,
		TypeHomomorphicHash,
		TypePayloadChecksum,
		TypeIntegrity,
		TypeStorageGroup,
		TypePublicKey,
	} {
		require.True(t, SupportedType(typ))
	}

	for _, typ := range []Type{
		lowerUndefined,
		upperUndefined,
		object.TypeFromUint32(object.TypeToUint32(lowerUndefined) - 1),
		object.TypeFromUint32(object.TypeToUint32(upperUndefined) + 1),
	} {
		require.False(t, SupportedType(typ))
	}
}
