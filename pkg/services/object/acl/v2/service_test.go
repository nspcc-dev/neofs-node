package v2_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	aclsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type mockFSChain struct{}

func (x *mockFSChain) InvokeContainedScript(*transaction.Transaction, *block.Header, *trigger.Type, *bool) (*result.Invoke, error) {
	panic("unimplemented")
}

func (x *mockFSChain) InContainerInLastTwoEpochs(cid.ID, []byte) (bool, error) {
	return false, nil
}

type mockIR struct {
}

func (x *mockIR) InnerRingKeys() ([][]byte, error) {
	return nil, nil
}

type mockContainers struct {
	cnrs map[cid.ID]container.Container
}

func (x *mockContainers) Get(id cid.ID) (container.Container, error) {
	cnr, ok := x.cnrs[id]
	if !ok {
		return container.Container{}, apistatus.ErrContainerNotFound
	}
	return cnr, nil
}

type mockNetmapper struct {
	curEpoch uint64
}

func (x *mockNetmapper) GetNetMapByEpoch(uint64) (*netmap.NetMap, error) {
	panic("unimplemented")
}

func (x *mockNetmapper) Epoch() (uint64, error) {
	return x.curEpoch, nil
}

func (x *mockNetmapper) NetMap() (*netmap.NetMap, error) {
	panic("unimplemented")
}

func (x *mockNetmapper) ServerInContainer(cid.ID) (bool, error) {
	return false, nil
}

func (x *mockNetmapper) GetEpochBlock(uint64) (uint32, error) {
	panic("unimplemented")
}

func testBearerTokenIssuer[REQ any](t *testing.T, exec func(*aclsvc.Service, REQ) (aclsvc.RequestInfo, error),
	signRequest func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) REQ,
) {
	var err error
	cnrID := cidtest.ID()
	owner := usertest.User()
	sender := usertest.User()

	var cnr container.Container
	cnr.SetOwner(owner.ID)

	otherCnrID := cidtest.OtherID(cnrID)
	var otherCnr container.Container
	otherCnr.SetOwner(sender.ID)

	cnrs := mockContainers{
		cnrs: map[cid.ID]container.Container{
			cnrID:      cnr,
			otherCnrID: otherCnr,
		},
	}

	var fsChain mockFSChain
	var nm mockNetmapper
	var ir mockIR
	svc := aclsvc.New(&fsChain,
		aclsvc.WithContainerSource(&cnrs),
		aclsvc.WithNetmapper(&nm),
		aclsvc.WithIRFetcher(&ir),
	)

	var bt bearer.Token
	bt.SetEACLTable(eacl.Table{}) // any
	bt.SetExp(1)
	require.NoError(t, bt.Sign(owner))

	meta := &protosession.RequestMetaHeader{
		BearerToken: bt.ProtoMessage(),
	}

	call := func(t *testing.T, cnrID cid.ID) error {
		req := signRequest(t, sender, cnrID, meta)

		_, err = exec(&svc, req)
		return err
	}

	t.Run("not a container owner", func(t *testing.T) {
		err := call(t, otherCnrID)
		require.EqualError(t, err, "status: code = 2048 message = access to object operation denied")
		var accessDenied apistatus.ObjectAccessDenied
		require.ErrorAs(t, err, &accessDenied)
		require.Equal(t, "bearer token issuer differs from the container owner", accessDenied.Reason())
	})

	err = call(t, cnrID)
	require.NoError(t, err)
}

func TestService_HeadRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).HeadRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.HeadRequest {
		req := &protoobject.HeadRequest{
			Body: &protoobject.HeadRequest_Body{
				Address: &refs.Address{
					ContainerId: cnrID.ProtoMessage(),
					ObjectId:    oidtest.ID().ProtoMessage(),
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_GetRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).GetRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.GetRequest {
		req := &protoobject.GetRequest{
			Body: &protoobject.GetRequest_Body{
				Address: &refs.Address{
					ContainerId: cnrID.ProtoMessage(),
					ObjectId:    oidtest.ID().ProtoMessage(),
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_SearchRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).SearchRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.SearchRequest {
		req := &protoobject.SearchRequest{
			Body: &protoobject.SearchRequest_Body{
				ContainerId: cnrID.ProtoMessage(),
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_SearchV2RequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).SearchV2RequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.SearchV2Request {
		req := &protoobject.SearchV2Request{
			Body: &protoobject.SearchV2Request_Body{
				ContainerId: cnrID.ProtoMessage(),
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_DeleteRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).DeleteRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.DeleteRequest {
		req := &protoobject.DeleteRequest{
			Body: &protoobject.DeleteRequest_Body{
				Address: &refs.Address{
					ContainerId: cnrID.ProtoMessage(),
					ObjectId:    oidtest.ID().ProtoMessage(),
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_RangeRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).RangeRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.GetRangeRequest {
		req := &protoobject.GetRangeRequest{
			Body: &protoobject.GetRangeRequest_Body{
				Address: &refs.Address{
					ContainerId: cnrID.ProtoMessage(),
					ObjectId:    oidtest.ID().ProtoMessage(),
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_HashRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, (*aclsvc.Service).HashRequestToInfo, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.GetRangeHashRequest {
		req := &protoobject.GetRangeHashRequest{
			Body: &protoobject.GetRangeHashRequest_Body{
				Address: &refs.Address{
					ContainerId: cnrID.ProtoMessage(),
					ObjectId:    oidtest.ID().ProtoMessage(),
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}

func TestService_PutRequestToInfo_BearerTokenIssuer(t *testing.T) {
	testBearerTokenIssuer(t, func(svc *aclsvc.Service, req *protoobject.PutRequest) (aclsvc.RequestInfo, error) {
		res, _, err := svc.PutRequestToInfo(req)
		return res, err
	}, func(t *testing.T, signer neofscrypto.Signer, cnrID cid.ID, meta *protosession.RequestMetaHeader) *protoobject.PutRequest {
		req := &protoobject.PutRequest{
			Body: &protoobject.PutRequest_Body{
				ObjectPart: &protoobject.PutRequest_Body_Init_{
					Init: &protoobject.PutRequest_Body_Init{
						Header: &protoobject.Header{
							ContainerId: cnrID.ProtoMessage(),
							OwnerId:     usertest.ID().ProtoMessage(),
						},
					},
				},
			},
			MetaHeader: meta,
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
		require.NoError(t, err)

		return req
	})
}
