package tree

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapAPI "github.com/nspcc-dev/neofs-sdk-go/netmap"
	ownerSDK "github.com/nspcc-dev/neofs-sdk-go/owner"
	bearerSDK "github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type dummyNetmapSource struct {
	netmap.Source
}

type dummyContainerSource map[string]*container.Container

func (s dummyContainerSource) Get(id *cid.ID) (*container.Container, error) {
	cnt, ok := s[id.String()]
	if !ok {
		return nil, errors.New("container not found")
	}
	return cnt, nil
}

func testContainer(owner *ownerSDK.ID) *container.Container {
	r := netmapAPI.NewReplica()
	r.SetCount(1)

	pp := netmapAPI.NewPlacementPolicy()
	pp.SetReplicas(*r)

	cnt := container.New()
	cnt.SetOwnerID(owner)
	cnt.SetPlacementPolicy(pp)

	return cnt
}

func TestMessageSign(t *testing.T) {
	privs := make([]*keys.PrivateKey, 4)
	for i := range privs {
		p, err := keys.NewPrivateKey()
		require.NoError(t, err)
		privs[i] = p
	}

	cid1 := cidtest.ID()
	cid2 := cidtest.ID()
	ownerID := ownerSDK.NewIDFromPublicKey((*ecdsa.PublicKey)(privs[0].PublicKey()))

	s := &Service{
		cfg: cfg{
			log:      zaptest.NewLogger(t),
			key:      &privs[0].PrivateKey,
			nmSource: dummyNetmapSource{},
			cnrSource: dummyContainerSource{
				cid1.String(): testContainer(ownerID),
			},
		},
	}

	req := &MoveRequest{
		Body: &MoveRequest_Body{
			ContainerId: cid1.ToV2().GetValue(),
			ParentId:    1,
			NodeId:      2,
			Meta: []*KeyValue{
				{Key: "kkk", Value: []byte("vvv")},
			},
		},
	}

	t.Run("missing signature, no panic", func(t *testing.T) {
		require.Error(t, s.verifyClient(req, cid2, nil, eaclSDK.OperationUnknown))
	})

	require.NoError(t, signMessage(req, &privs[0].PrivateKey))
	require.NoError(t, s.verifyClient(req, cid1, nil, eaclSDK.OperationUnknown))

	t.Run("invalid CID", func(t *testing.T) {
		require.Error(t, s.verifyClient(req, cid2, nil, eaclSDK.OperationUnknown))
	})
	t.Run("invalid key", func(t *testing.T) {
		require.NoError(t, signMessage(req, &privs[1].PrivateKey))
		require.Error(t, s.verifyClient(req, cid1, nil, eaclSDK.OperationUnknown))
	})

	t.Run("bearer", func(t *testing.T) {
		t.Run("invalid bearer", func(t *testing.T) {
			req.Body.BearerToken = []byte{0xFF}
			require.NoError(t, signMessage(req, &privs[0].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
		})

		t.Run("invalid bearer CID", func(t *testing.T) {
			bt := testBearerToken(cid2, ownerID, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.SignToken(&privs[0].PrivateKey))
			req.Body.BearerToken, _ = bt.Marshal()

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
		})
		t.Run("invalid bearer owner", func(t *testing.T) {
			owner2 := ownerSDK.NewIDFromPublicKey((*ecdsa.PublicKey)(privs[1].PublicKey()))
			bt := testBearerToken(cid1, owner2, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.SignToken(&privs[1].PrivateKey))
			req.Body.BearerToken, _ = bt.Marshal()

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
		})
		t.Run("invalid bearer signature", func(t *testing.T) {
			bt := testBearerToken(cid1, ownerID, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.SignToken(&privs[0].PrivateKey))
			bv2 := bt.ToV2()
			bv2.GetSignature().SetSign([]byte{1, 2, 3})
			req.Body.BearerToken = bv2.StableMarshal(nil)

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
		})

		bt := testBearerToken(cid1, ownerID, privs[1].PublicKey(), privs[2].PublicKey())
		require.NoError(t, bt.SignToken(&privs[0].PrivateKey))
		req.Body.BearerToken, _ = bt.Marshal()

		t.Run("put and get", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationGet))
		})
		t.Run("only get", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[2].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationGet))
		})
		t.Run("none", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[3].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationPut))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), eaclSDK.OperationGet))
		})
	})
}

func testBearerToken(cid *cid.ID, ownerID *ownerSDK.ID, forPut, forGet *keys.PublicKey) *bearerSDK.BearerToken {
	tgtGet := eaclSDK.NewTarget()
	tgtGet.SetRole(eaclSDK.RoleUnknown)
	tgtGet.SetBinaryKeys([][]byte{forPut.Bytes(), forGet.Bytes()})

	rGet := eaclSDK.NewRecord()
	rGet.SetAction(eaclSDK.ActionAllow)
	rGet.SetOperation(eaclSDK.OperationGet)
	rGet.SetTargets(*tgtGet)

	tgtPut := eaclSDK.NewTarget()
	tgtPut.SetRole(eaclSDK.RoleUnknown)
	tgtPut.SetBinaryKeys([][]byte{forPut.Bytes()})

	rPut := eaclSDK.NewRecord()
	rPut.SetAction(eaclSDK.ActionAllow)
	rPut.SetOperation(eaclSDK.OperationPut)
	rPut.SetTargets(*tgtPut)

	tb := eaclSDK.NewTable()
	tb.AddRecord(rGet)
	tb.AddRecord(rPut)

	tgt := eaclSDK.NewTarget()
	tgt.SetRole(eaclSDK.RoleOthers)

	for _, op := range []eaclSDK.Operation{eaclSDK.OperationGet, eaclSDK.OperationPut} {
		r := eaclSDK.NewRecord()
		r.SetAction(eaclSDK.ActionDeny)
		r.SetTargets(*tgt)
		r.SetOperation(op)
		tb.AddRecord(r)
	}

	tb.SetCID(cid)

	b := bearerSDK.NewBearerToken()
	b.SetOwner(ownerID)
	b.SetEACLTable(tb)

	return b
}
