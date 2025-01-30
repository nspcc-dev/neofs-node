package tree

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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

func (s dummyContainerSource) List() ([]cid.ID, error) {
	res := make([]cid.ID, 0, len(s))
	var cnr cid.ID

	for cidStr := range s {
		err := cnr.DecodeString(cidStr)
		if err != nil {
			return nil, err
		}

		res = append(res, cnr)
	}

	return res, nil
}

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
	pp.SetReplicas([]netmapSDK.ReplicaDescriptor{r})

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

	signer := user.NewAutoIDSignerRFC6979(privs[0].PrivateKey)

	ownerID := user.NewFromECDSAPublicKey(privs[0].PrivateKey.PublicKey)

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

	req := &MoveRequest{
		Body: &MoveRequest_Body{
			ContainerId: cid1[:],
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

	require.NoError(t, SignMessage(req, &privs[0].PrivateKey))
	require.NoError(t, s.verifyClient(req, cid1, nil, op))

	t.Run("invalid CID", func(t *testing.T) {
		require.Error(t, s.verifyClient(req, cid2, nil, op))
	})

	cnr.Value.SetBasicACL(acl.Private)

	t.Run("extension disabled", func(t *testing.T) {
		require.NoError(t, SignMessage(req, &privs[0].PrivateKey))
		require.Error(t, s.verifyClient(req, cid2, nil, op))
	})

	t.Run("invalid key", func(t *testing.T) {
		require.NoError(t, SignMessage(req, &privs[1].PrivateKey))
		require.Error(t, s.verifyClient(req, cid1, nil, op))
	})

	t.Run("bearer", func(t *testing.T) {
		bACL := acl.PrivateExtended
		bACL.AllowBearerRules(op)
		cnr.Value.SetBasicACL(bACL)

		bACL.DisableExtension()

		t.Run("invalid bearer", func(t *testing.T) {
			req.Body.BearerToken = []byte{0xFF}
			require.NoError(t, SignMessage(req, &privs[0].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})

		t.Run("invalid bearer CID", func(t *testing.T) {
			bt := testBearerToken(cid2, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(signer))
			req.Body.BearerToken = bt.Marshal()

			require.NoError(t, SignMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})
		t.Run("invalid bearer owner", func(t *testing.T) {
			bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(signer))
			req.Body.BearerToken = bt.Marshal()

			require.NoError(t, SignMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})
		t.Run("invalid bearer signature", func(t *testing.T) {
			bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
			require.NoError(t, bt.Sign(signer))

			pacl := bt.ProtoMessage()
			pacl.Signature.Sign = []byte{1, 2, 3}
			b := make([]byte, pacl.MarshaledSize())
			pacl.MarshalStable(b)
			req.Body.BearerToken = b

			require.NoError(t, SignMessage(req, &privs[1].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
		})

		bt := testBearerToken(cid1, privs[1].PublicKey(), privs[2].PublicKey())
		require.NoError(t, bt.Sign(signer))
		req.Body.BearerToken = bt.Marshal()
		cnr.Value.SetBasicACL(acl.PublicRWExtended)

		t.Run("put and get", func(t *testing.T) {
			require.NoError(t, SignMessage(req, &privs[1].PrivateKey))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
		t.Run("only get", func(t *testing.T) {
			require.NoError(t, SignMessage(req, &privs[2].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.NoError(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
		t.Run("none", func(t *testing.T) {
			require.NoError(t, SignMessage(req, &privs[3].PrivateKey))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectPut))
			require.Error(t, s.verifyClient(req, cid1, req.GetBody().GetBearerToken(), acl.OpObjectGet))
		})
	})
}

func testBearerToken(cid cid.ID, forPutGet, forGet *keys.PublicKey) bearer.Token {
	tgtGet := eaclSDK.NewTargetByRole(eaclSDK.RoleUnspecified)
	tgtGet.SetRawSubjects([][]byte{forPutGet.Bytes(), forGet.Bytes()})

	rGet := eaclSDK.ConstructRecord(eaclSDK.ActionAllow, eaclSDK.OperationGet, []eaclSDK.Target{tgtGet})

	tgtPut := eaclSDK.NewTargetByRole(eaclSDK.RoleUnspecified)
	tgtPut.SetRawSubjects([][]byte{forPutGet.Bytes()})

	rPut := eaclSDK.ConstructRecord(eaclSDK.ActionAllow, eaclSDK.OperationPut, []eaclSDK.Target{tgtPut})

	tb := eaclSDK.ConstructTable([]eaclSDK.Record{rGet, rPut})

	tgt := eaclSDK.NewTargetByRole(eaclSDK.RoleOthers)

	for _, op := range []eaclSDK.Operation{eaclSDK.OperationGet, eaclSDK.OperationPut} {
		r := eaclSDK.ConstructRecord(eaclSDK.ActionDeny, op, []eaclSDK.Target{tgt})
		tb.SetRecords(append(tb.Records(), r))
	}

	tb.SetCID(cid)

	var b bearer.Token
	b.SetEACLTable(tb)

	return b
}
