package audit_test

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/stretchr/testify/require"
)

func TestAuditResults(t *testing.T) {
	t.Skip()
	const epoch = 11

	endpoint := "http://morph_chain.neofs.devenv:30333"
	sAuditHash := "cdfb3dab86e6d60e8a143d9e2ecb0b188f3dc2eb"
	irKeyWIF := "L3o221BojgcCPYgdbXsm6jn7ayTZ72xwREvBHXKknR8VJ3G4WmjB"

	key, err := crypto.WIFDecode(irKeyWIF)
	require.NoError(t, err)

	pubKey := crypto.MarshalPublicKey(&key.PublicKey)

	auditHash, err := util.Uint160DecodeStringLE(sAuditHash)
	require.NoError(t, err)

	morphClient, err := client.New(key, endpoint)
	require.NoError(t, err)

	auditContractClient, err := client.NewStatic(morphClient, auditHash, 0)
	require.NoError(t, err)

	auditClient := audit.New(auditContractClient)

	auditClientWrapper := auditWrapper.WrapClient(auditClient)

	cid := container.NewID()
	cid.SetSHA256([sha256.Size]byte{1, 2, 3})

	auditRes := auditAPI.NewResult()
	auditRes.SetAuditEpoch(epoch)
	auditRes.SetPublicKey(pubKey)
	auditRes.SetContainerID(cid)

	require.NoError(t, auditClientWrapper.PutAuditResult(auditRes))

	time.Sleep(5 * time.Second)

	list, err := auditClientWrapper.ListAuditResultIDByCID(epoch, cid)
	require.NoError(t, err)
	require.Len(t, list, 1)

	savedAuditRes, err := auditClientWrapper.GetAuditResult(list[0])
	require.NoError(t, err)

	require.Equal(t, auditRes, savedAuditRes)
}
