package audit

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditAPI "github.com/nspcc-dev/neofs-sdk-go/audit"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
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

	morphClient, err := client.New(key, client.WithEndpoints([]string{endpoint}))
	require.NoError(t, err)

	auditClientWrapper, err := NewFromMorph(morphClient, auditHash, 0)
	require.NoError(t, err)

	id := cidtest.ID()

	var auditRes auditAPI.Result
	auditRes.ForEpoch(epoch)
	auditRes.SetAuditorKey(key.PublicKey().Bytes())
	auditRes.ForContainer(id)

	prm := PutPrm{}
	prm.SetResult(&auditRes)

	require.NoError(t, auditClientWrapper.PutAuditResult(prm))

	time.Sleep(5 * time.Second)

	list, err := auditClientWrapper.ListAuditResultIDByCID(epoch, id)
	require.NoError(t, err)
	require.Len(t, list, 1)

	savedAuditRes, err := auditClientWrapper.GetAuditResult(list[0])
	require.NoError(t, err)

	require.Equal(t, auditRes, savedAuditRes)
}
