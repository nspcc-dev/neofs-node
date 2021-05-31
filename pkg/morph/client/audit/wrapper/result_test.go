package audit_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/stretchr/testify/require"
)

func TestAuditResults(t *testing.T) {
	t.Skip()
	const epoch = 11

	endpoint := "http://morph_chain.neofs.devenv:30333"
	sAuditHash := "cdfb3dab86e6d60e8a143d9e2ecb0b188f3dc2eb"
	irKeyWIF := "L3o221BojgcCPYgdbXsm6jn7ayTZ72xwREvBHXKknR8VJ3G4WmjB"

	key, err := keys.NewPrivateKeyFromWIF(irKeyWIF)
	require.NoError(t, err)

	auditHash, err := util.Uint160DecodeStringLE(sAuditHash)
	require.NoError(t, err)

	morphClient, err := client.New(key, endpoint)
	require.NoError(t, err)

	auditClientWrapper, err := auditWrapper.NewFromMorph(morphClient, auditHash, 0)
	require.NoError(t, err)

	id := cidtest.Generate()

	auditRes := auditAPI.NewResult()
	auditRes.SetAuditEpoch(epoch)
	auditRes.SetPublicKey(key.PublicKey().Bytes())
	auditRes.SetContainerID(id)

	require.NoError(t, auditClientWrapper.PutAuditResult(auditRes))

	time.Sleep(5 * time.Second)

	list, err := auditClientWrapper.ListAuditResultIDByCID(epoch, id)
	require.NoError(t, err)
	require.Len(t, list, 1)

	savedAuditRes, err := auditClientWrapper.GetAuditResult(list[0])
	require.NoError(t, err)

	require.Equal(t, auditRes, savedAuditRes)
}
