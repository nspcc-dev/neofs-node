package container

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

func TestCheckFormat(t *testing.T) {
	c := container.New()

	require.Error(t, CheckFormat(c))

	policy := new(netmap.PlacementPolicy)
	c.SetPlacementPolicy(policy.ToV2())

	require.Error(t, CheckFormat(c))

	c.SetVersion(pkg.SDKVersion().ToV2())

	require.Error(t, CheckFormat(c))

	wallet, err := owner.NEO3WalletFromPublicKey(&test.DecodeKey(-1).PublicKey)
	require.NoError(t, err)

	c.SetOwnerID(owner.NewIDFromNeo3Wallet(wallet).ToV2())

	c.SetNonce(nil)

	require.Error(t, CheckFormat(c))

	uid, err := uuid.NewRandom()
	require.NoError(t, err)

	nonce, _ := uid.MarshalBinary()

	c.SetNonce(nonce)

	require.NoError(t, CheckFormat(c))
}
