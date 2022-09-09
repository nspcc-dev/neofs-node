package tree

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	aclV2 "github.com/nspcc-dev/neofs-api-go/v2/acl"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type dummyNetmapSource struct {
	netmap.Source
}

type dummyContainerSource map[string]*containercore.Container

func (s dummyContainerSource) Get(id cid.ID) (*containercore.Container, error) {
	cnt, ok := s[id.String()]
	if !ok {
		return nil, errors.New("container not found")
	}
	return cnt, nil
}

func testContainer(owner user.ID) container.Container {
	var r netmapSDK.ReplicaDescriptor
	r.SetNumberOfObjects(1)

	var pp netmapSDK.PlacementPolicy
	pp.AddReplicas(r)

	var cnt container.Container
	cnt.SetOwner(owner)
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

	var ownerID user.ID
	user.IDFromKey(&ownerID, (ecdsa.PublicKey)(*privs[0].PublicKey()))

	cnr := &containercore.Container{
		Value: testContainer(ownerID),
	}

	s := &Service{
		cfg: cfg{
			log:      zaptest.NewLogger(t),
			key:      &privs[0].PrivateKey,
			nmSource: dummyNetmapSource{},
			cnrSource: dummyContainerSource{
				cid1.String(): cnr,
			},
		},
	}

	rawCID1 := make([]byte, sha256.Size)
	cid1.Encode(rawCID1)

	req := &MoveRequest{
		Body: &MoveRequest_Body{
			ContainerId: rawCID1,
			ParentId:    1,
			NodeId:      2,
			Meta: []*KeyValue{
				{Key: "kkk", Value: []byte("vvv")},
			},
		},
	}

	op := acl.OpObjectPut
	cnr.Value.SetBasicACL(acl.PublicRW)

	t.Run("missing signature, no panic", func(t *testing.T) {
		require.Error(t, s.verifyClient(req, cid2, nil, op))
	})

	require.NoError(t, signMessage(req, &privs[0].PrivateKey))
	require.NoError(t, s.verifyClient(req, cid1, nil, op))

	t.Run("invalid CID", func(t *testing.T) {
		require.Error(t, s.verifyClient(req, cid2, nil, op))
	})

	cnr.Value.SetBasicACL(acl.Private)

	t.Run("extension disabled", func(t *testing.T) {
		require.NoError(t, signMessage(req, &privs[0].PrivateKey))
		require.Error(t, s.verifyClient(req, cid2, nil, op))
	})

	t.Run("invalid key", func(t *testing.T) {
		require.NoError(t, signMessage(req, &privs[1].PrivateKey))
		require.Error(t, s.verifyClient(req, cid1, nil, op))
	})

	t.Run("bearer", func(t *testing.T) {
		bACL := acl.PrivateExtended
		bACL.AllowBearerRules(op)
		cnr.Value.SetBasicACL(bACL)

		bACL.DisableExtension()

		t.Run("invalid bearer", func(t *testing.T) {
			req.Body.BearerToken = []byte{0xFF}
			require.NoError(t, signMessage(req, &privs[0].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})

		t.Run("invalid bearer CID", func(t *testing.T) {
			bt := testBearerToken(cid2, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(privs[0].PrivateKey))
			req.Body.BearerToken = bt.Marshal()

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})
		t.Run("invalid bearer owner", func(t *testing.T) {
			bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(privs[1].PrivateKey))
			req.Body.BearerToken = bt.Marshal()

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})
		t.Run("invalid bearer signature", func(t *testing.T) {
			bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(privs[0].PrivateKey))

			var bv2 aclV2.BearerToken
			bt.WriteToV2(&bv2)
			bv2.GetSignature().SetSign([]byte{1, 2, 3})
			req.Body.BearerToken = bv2.StableMarshal(nil)

			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})

		bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
		require.NoError(t, bt.Sign(privs[0].PrivateKey))
		req.Body.BearerToken = bt.Marshal()
		cnr.Value.SetBasicACL(acl.PublicRWExtended)

		t.Run("put and get", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[1].PrivateKey))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
		t.Run("only get", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[2].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
		t.Run("none", func(t *testing.T) {
			require.NoError(t, signMessage(req, &privs[3].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
	})
}

func testBearerToken(cid cid.ID, forPutGet, forGet *keys.PublicKey) bearer.Token {
	tgtGet := eaclSDK.NewTarget()
	tgtGet.SetRole(eaclSDK.RoleUnknown)
	tgtGet.SetBinaryKeys([][]byte{forPutGet.Bytes(), forGet.Bytes()})

	rGet := eaclSDK.NewRecord()
	rGet.SetAction(eaclSDK.ActionAllow)
	rGet.SetOperation(eaclSDK.OperationGet)
	rGet.SetTargets(*tgtGet)

	tgtPut := eaclSDK.NewTarget()
	tgtPut.SetRole(eaclSDK.RoleUnknown)
	tgtPut.SetBinaryKeys([][]byte{forPutGet.Bytes()})

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

	var b bearer.Token
	b.SetEACLTable(*tb)

	return b
}
