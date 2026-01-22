package container_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	containerSvc "github.com/nspcc-dev/neofs-node/pkg/services/container"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	eacltest "github.com/nspcc-dev/neofs-sdk-go/eacl/test"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type unimplementedFSChain struct{}

func (unimplementedFSChain) InvokeContainedScript(*transaction.Transaction, *block.Header, *trigger.Type, *bool) (*result.Invoke, error) {
	panic("unimplemented")
}

type unimplementedContainerContract struct{}

func (unimplementedContainerContract) Put(context.Context, container.Container, []byte, []byte, *session.Container) (cid.ID, error) {
	panic("implement me")
}

func (unimplementedContainerContract) Get(cid.ID) (container.Container, error) {
	panic("unimplemented")
}

func (unimplementedContainerContract) List(user.ID) ([]cid.ID, error) {
	panic("unimplemented")
}

func (unimplementedContainerContract) PutEACL(context.Context, eacl.Table, []byte, []byte, *session.Container) error {
	panic("unimplemented")
}

func (unimplementedContainerContract) GetEACL(cid.ID) (eacl.Table, error) {
	panic("unimplemented")
}

func (unimplementedContainerContract) Delete(context.Context, cid.ID, []byte, []byte, *session.Container) error {
	panic("unimplemented")
}

func (unimplementedContainerContract) SetAttribute(context.Context, cid.ID, string, string, uint64, []byte, []byte, []byte) error {
	panic("unimplemented")
}

func (unimplementedContainerContract) RemoveAttribute(context.Context, cid.ID, string, uint64, []byte, []byte, []byte) error {
	panic("unimplemented")
}

type unimplementedNetmapContract struct{}

func (unimplementedNetmapContract) GetEpochBlock(uint64) (uint32, error) {
	panic("unimplemented")
}

func (unimplementedNetmapContract) GetEpochBlockByTime(uint32) (uint32, error) {
	panic("unimplemented")
}

type testNodeState struct {
	epoch uint64
}

func (x testNodeState) CurrentEpoch() uint64 { return x.epoch }

type testFSChain struct {
	testNodeState
	getErr error
	cnrs   map[cid.ID]container.Container
}

func (testFSChain) Put(context.Context, container.Container, []byte, []byte, *session.Container) (cid.ID, error) {
	return cid.ID{}, errors.New("unimplemented")
}

func (x testFSChain) Get(id cid.ID) (container.Container, error) {
	if x.getErr != nil {
		return container.Container{}, x.getErr
	}
	cnr, ok := x.cnrs[id]
	if !ok {
		return container.Container{}, apistatus.ErrContainerNotFound
	}
	return cnr, nil
}

func (testFSChain) List(user.ID) ([]cid.ID, error) {
	return nil, errors.New("unimplemented")
}

func (testFSChain) PutEACL(context.Context, eacl.Table, []byte, []byte, *session.Container) error {
	return nil
}

func (testFSChain) GetEACL(cid.ID) (eacl.Table, error) {
	return eacl.Table{}, errors.New("unimplemented")
}

func (testFSChain) Delete(context.Context, cid.ID, []byte, []byte, *session.Container) error {
	return nil
}

func (x testFSChain) GetEpochBlock(uint64) (uint32, error) { panic("unimplemented") }

func (x testFSChain) GetEpochBlockByTime(uint32) (uint32, error) {
	panic("unimplemented")
}

func (x testFSChain) InvokeContainedScript(*transaction.Transaction, *block.Header, *trigger.Type, *bool) (*result.Invoke, error) {
	panic("unimplemented")
}

func (testFSChain) SetAttribute(context.Context, cid.ID, string, string, uint64, []byte, []byte, []byte) error {
	return errors.New("unimplemented")
}

func (testFSChain) RemoveAttribute(context.Context, cid.ID, string, uint64, []byte, []byte, []byte) error {
	return errors.New("unimplemented")
}

func makeDeleteRequestWithSession(t testing.TB, usr usertest.UserSigner, cnr cid.ID, st interface {
	ProtoMessage() *protosession.SessionToken
}) *protocontainer.DeleteRequest {
	return makeDeleteRequestWithSessionMessage(t, usr, cnr, st.ProtoMessage())
}

func makeDeleteRequestWithSessionMessage(t testing.TB, usr usertest.UserSigner, cnr cid.ID, st *protosession.SessionToken) *protocontainer.DeleteRequest {
	var err error

	req := &protocontainer.DeleteRequest{
		Body: &protocontainer.DeleteRequest_Body{
			ContainerId: cnr.ProtoMessage(),
			Signature:   new(refs.SignatureRFC6979),
		},
		MetaHeader: &protosession.RequestMetaHeader{
			SessionToken: st,
		},
	}

	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofsecdsa.Signer(usr.ECDSAPrivateKey), req, nil)
	require.NoError(t, err)

	return req
}

func TestServer_Delete(t *testing.T) {
	ctx := context.Background()
	usr := usertest.User()
	id := cidtest.ID()

	const anyEpoch = 10
	var cnr container.Container
	cnr.SetOwner(usr.ID)

	m := &testFSChain{
		cnrs: map[cid.ID]container.Container{
			id: cnr,
		},
	}
	m.epoch = anyEpoch
	svc := containerSvc.New(&usr.ECDSAPrivateKey, m, m, m, m)

	t.Run("session", func(t *testing.T) {
		t.Run("failure", func(t *testing.T) {
			t.Run("non-container", func(t *testing.T) {
				var st session.Object
				st.SetIssuer(usr.ID)
				st.SetID(uuid.New())
				st.SetAuthKey(neofscryptotest.Signer().Public())
				st.SetIat(anyEpoch)
				st.SetNbf(anyEpoch)
				st.SetExp(anyEpoch)
				require.NoError(t, st.Sign(usr))

				req := makeDeleteRequestWithSession(t, usr, cidtest.ID(), st)
				resp, err := svc.Delete(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Nil(t, resp.Body)

				require.NotNil(t, resp.MetaHeader)
				require.NotNil(t, resp.MetaHeader.Status)
				sts := resp.MetaHeader.Status
				require.EqualValues(t, 1024, sts.Code, st)
				require.Equal(t, "invalid context *session.SessionToken_Body_Object", sts.Message)
				require.Zero(t, sts.Details)
			})
			t.Run("wrong verb", func(t *testing.T) {
				var st session.Container
				st.SetIssuer(usr.ID)
				st.SetID(uuid.New())
				st.SetAuthKey(neofscryptotest.Signer().Public())
				st.SetIat(anyEpoch)
				st.SetNbf(anyEpoch)
				st.SetExp(anyEpoch)

				st.ForVerb(session.VerbContainerPut)

				require.NoError(t, st.Sign(usr))

				req := makeDeleteRequestWithSession(t, usr, cidtest.ID(), st)
				resp, err := svc.Delete(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Nil(t, resp.Body)

				require.NotNil(t, resp.MetaHeader)
				require.NotNil(t, resp.MetaHeader.Status)
				sts := resp.MetaHeader.Status
				require.EqualValues(t, 1024, sts.Code, st)
				require.Equal(t, "wrong container session operation", sts.Message)
				require.Zero(t, sts.Details)
			})
			t.Run("container ID mismatch", func(t *testing.T) {
				var st session.Container
				st.SetIssuer(usr.ID)
				st.SetID(uuid.New())
				st.SetAuthKey(neofscryptotest.Signer().Public())
				st.SetIat(anyEpoch)
				st.SetNbf(anyEpoch)
				st.SetExp(anyEpoch)
				st.ForVerb(session.VerbContainerDelete)

				reqCnr := cidtest.ID()
				st.ApplyOnlyTo(cidtest.OtherID(reqCnr))

				require.NoError(t, st.Sign(usr))

				req := makeDeleteRequestWithSession(t, usr, reqCnr, st)
				resp, err := svc.Delete(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Nil(t, resp.Body)

				require.NotNil(t, resp.MetaHeader)
				require.NotNil(t, resp.MetaHeader.Status)
				sts := resp.MetaHeader.Status
				require.EqualValues(t, 1024, sts.Code, st)
				require.Equal(t, "session is not applied to requested container", sts.Message)
				require.Zero(t, sts.Details)
			})
			t.Run("non-owner issuer", func(t *testing.T) {
				var st session.Container
				st.SetID(uuid.New())
				st.SetAuthKey(neofscryptotest.Signer().Public())
				st.SetIat(anyEpoch)
				st.SetNbf(anyEpoch)
				st.SetExp(anyEpoch)
				st.ForVerb(session.VerbContainerDelete)

				otherUsr := usertest.User()
				require.NoError(t, st.Sign(otherUsr))

				req := makeDeleteRequestWithSession(t, usr, id, st)
				resp, err := svc.Delete(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Nil(t, resp.Body)

				require.NotNil(t, resp.MetaHeader)
				require.NotNil(t, resp.MetaHeader.Status)
				sts := resp.MetaHeader.Status
				require.EqualValues(t, 1024, sts.Code, st)
				require.Equal(t, "session was not issued by the container owner", sts.Message)
				require.Zero(t, sts.Details)
			})
			t.Run("incorrect signature", func(t *testing.T) {
				var st session.Container
				st.SetID(uuid.New())
				st.SetAuthKey(neofscryptotest.Signer().Public())
				st.SetIat(anyEpoch)
				st.SetNbf(anyEpoch)
				st.SetExp(anyEpoch)
				st.ForVerb(session.VerbContainerDelete)
				require.NoError(t, st.Sign(usr))

				mst := st.ProtoMessage()
				require.NotEmpty(t, mst.Signature.Sign)
				mst.Signature.Sign[0]++

				req := makeDeleteRequestWithSessionMessage(t, usr, cidtest.ID(), mst)
				resp, err := svc.Delete(ctx, req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Nil(t, resp.Body)

				require.NotNil(t, resp.MetaHeader)
				require.NotNil(t, resp.MetaHeader.Status)
				sts := resp.MetaHeader.Status
				require.EqualValues(t, 1024, sts.Code, st)
				require.Equal(t, "signature mismatch", sts.Message)
				require.Zero(t, sts.Details)
			})
			t.Run("token lifetime", func(t *testing.T) {
				for _, tc := range []struct {
					name               string
					iat, nbf, exp, cur uint64
					code               uint32
					msg                string
				}{
					{name: "iat future", iat: 11, nbf: 10, exp: 10, cur: 10, code: 1024,
						msg: "token should not be issued yet: IAt: 11, current epoch: 10"},
					{name: "nbf future", iat: 10, nbf: 11, exp: 10, cur: 10, code: 1024,
						msg: "token is not valid yet: NBf: 11, current epoch: 10"},
					{name: "expired", iat: 10, nbf: 10, exp: 9, cur: 10, code: 4097,
						msg: "expired session token"},
				} {
					var st session.Container
					st.SetIssuer(usr.ID)
					st.SetID(uuid.New())
					st.SetAuthKey(neofscryptotest.Signer().Public())
					st.ForVerb(session.VerbContainerDelete)

					m.epoch = tc.cur
					st.SetIat(tc.iat)
					st.SetNbf(tc.nbf)
					st.SetExp(tc.exp)

					require.NoError(t, st.Sign(usr))

					req := makeDeleteRequestWithSession(t, usr, cidtest.ID(), st)
					resp, err := svc.Delete(ctx, req)
					require.NoError(t, err)
					require.NotNil(t, resp)
					require.Nil(t, resp.Body)

					require.NotNil(t, resp.MetaHeader)
					require.NotNil(t, resp.MetaHeader.Status)
					sts := resp.MetaHeader.Status
					require.EqualValues(t, tc.code, sts.Code, st)
					require.Equal(t, tc.msg, sts.Message)
					require.Zero(t, sts.Details)
				}
			})
		})
	})
}

func TestSessionVerb(t *testing.T) {
	var err error
	owner := usertest.User()
	cnrID := cidtest.ID()

	var cnr container.Container
	cnr.SetOwner(owner.ID)

	var fsChain testFSChain
	fsChain.cnrs = map[cid.ID]container.Container{
		cnrID: cnr,
	}

	s := containerSvc.New(&owner.ECDSAPrivateKey, fsChain, fsChain, fsChain, fsChain)

	var st session.Container
	st.SetID(uuid.New())
	st.SetExp(fsChain.epoch + 1)
	st.SetAuthKey(neofscryptotest.Signer().Public())
	st.ForVerb(session.VerbContainerDelete)
	require.NoError(t, st.Sign(owner))

	t.Run("other op", func(t *testing.T) {
		eACL := eacltest.Table()
		eACLSig, err := owner.RFC6979.Sign(eACL.SignedData())
		require.NoError(t, err)

		setEACLReq := &protocontainer.SetExtendedACLRequest{
			Body: &protocontainer.SetExtendedACLRequest_Body{
				Eacl: eACL.ProtoMessage(),
				Signature: &refs.SignatureRFC6979{
					Key:  owner.PublicKeyBytes,
					Sign: eACLSig,
				},
			},
			MetaHeader: &protosession.RequestMetaHeader{
				SessionToken: st.ProtoMessage(),
			},
		}
		setEACLReq.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(owner, setEACLReq, nil)
		require.NoError(t, err)

		setEAClResp, err := s.SetExtendedACL(context.Background(), setEACLReq)
		require.NoError(t, err)
		require.NotNil(t, setEAClResp)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(setEAClResp, nil))
		require.Nil(t, setEAClResp.Body)
		require.NotNil(t, setEAClResp.MetaHeader)
		require.NotZero(t, setEAClResp.MetaHeader.Status)
		require.EqualValues(t, 1024, setEAClResp.MetaHeader.Status.Code)
		require.Equal(t, "wrong container session operation", setEAClResp.MetaHeader.Status.Message)
	})

	cidSig, err := owner.RFC6979.Sign(cnrID[:])
	require.NoError(t, err)

	delReq := &protocontainer.DeleteRequest{
		Body: &protocontainer.DeleteRequest_Body{
			ContainerId: cnrID.ProtoMessage(),
			Signature: &refs.SignatureRFC6979{
				Key:  owner.PublicKeyBytes,
				Sign: cidSig,
			},
		},
		MetaHeader: &protosession.RequestMetaHeader{
			SessionToken: st.ProtoMessage(),
		},
	}
	delReq.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(owner, delReq, nil)
	require.NoError(t, err)

	delResp, err := s.Delete(context.Background(), delReq)
	require.NoError(t, err)
	require.NotNil(t, delResp)
	require.Nil(t, delResp.Body)
	require.NoError(t, neofscrypto.VerifyResponseWithBuffer(delResp, nil))
	require.NotNil(t, delResp.MetaHeader)
	require.Zero(t, delResp.MetaHeader.Status)
}

func TestServer_SetExtendedACL_InvalidRequest(t *testing.T) {
	ctx := context.Background()
	usr := usertest.User()
	const currentEpoch = 10

	state := &testNodeState{
		epoch: currentEpoch,
	}
	var fsChain unimplementedFSChain
	var cnrContract unimplementedContainerContract
	var nmContract unimplementedNetmapContract

	// fsChain is used for response, other components must not be accessed for invalid request
	svc := containerSvc.New(&usr.ECDSAPrivateKey, state, fsChain, cnrContract, nmContract)

	t.Run("eACL without container ID", func(t *testing.T) {
		req := &protocontainer.SetExtendedACLRequest{
			Body: &protocontainer.SetExtendedACLRequest_Body{
				Eacl: &protoacl.EACLTable{
					ContainerId: nil,
				},
				Signature: &refs.SignatureRFC6979{},
			},
		}

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(usr, req, nil)
		require.NoError(t, err)

		resp, err := svc.SetExtendedACL(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))
		resp.VerifyHeader = nil

		require.True(t, proto.Equal(resp, &protocontainer.SetExtendedACLResponse{
			MetaHeader: &protosession.ResponseMetaHeader{
				Version: version.Current().ProtoMessage(),
				Epoch:   currentEpoch,
				Status: &protostatus.Status{
					Code:    1024,
					Message: "missing container ID in eACL table",
				},
			},
		}))
	})
}

func TestService_SetExtendedACL_SessionIssuer(t *testing.T) {
	ctx := context.Background()
	const currentEpoch = 10

	owner := usertest.User()
	id := cidtest.ID()
	var cnr container.Container
	cnr.SetOwner(owner.ID)

	otherID := cidtest.OtherID(id)
	var otherCnr container.Container
	otherCnr.SetOwner(usertest.OtherID(owner.ID))

	var fsChain testFSChain
	fsChain.epoch = currentEpoch
	fsChain.cnrs = map[cid.ID]container.Container{
		id:      cnr,
		otherID: otherCnr,
	}

	s := containerSvc.New(&owner.ECDSAPrivateKey, &fsChain, &fsChain, &fsChain, &fsChain)

	eACL := eacltest.Table()

	var st session.Container
	st.SetID(uuid.New())
	st.SetExp(fsChain.epoch + 1)
	st.SetAuthKey(neofscryptotest.Signer().Public())
	st.ForVerb(session.VerbContainerSetEACL)
	require.NoError(t, st.Sign(owner))

	req := &protocontainer.SetExtendedACLRequest{
		Body: &protocontainer.SetExtendedACLRequest_Body{
			Signature: &refs.SignatureRFC6979{},
		},
		MetaHeader: &protosession.RequestMetaHeader{
			SessionToken: st.ProtoMessage(),
		},
	}

	call := func(t *testing.T, id cid.ID, signer usertest.UserSigner) *protocontainer.SetExtendedACLResponse {
		eACL.SetCID(id)
		req.Body.Eacl = eACL.ProtoMessage()

		var err error
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(owner, req, nil)
		require.NoError(t, err)

		resp, err := s.SetExtendedACL(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))

		req.VerifyHeader = nil

		resp.VerifyHeader = nil
		return resp
	}

	assertResp := func(t *testing.T, resp *protocontainer.SetExtendedACLResponse, st *protostatus.Status) {
		require.True(t, proto.Equal(resp, &protocontainer.SetExtendedACLResponse{
			MetaHeader: &protosession.ResponseMetaHeader{
				Version: version.Current().ProtoMessage(),
				Epoch:   currentEpoch,
				Status:  st,
			},
		}), resp.GetMetaHeader().GetStatus())
	}

	assertOK := func(t *testing.T) {
		resp := call(t, id, owner)
		assertResp(t, resp, nil)
	}

	t.Run("unavailable container", func(t *testing.T) {
		someError := errors.New("some error")
		fsChain.getErr = someError

		resp := call(t, id, owner)

		assertResp(t, resp, &protostatus.Status{
			Code:    1024,
			Message: someError.Error(),
		})

		fsChain.getErr = nil
	})

	assertOK(t)

	t.Run("not a container owner", func(t *testing.T) {
		resp := call(t, otherID, owner)

		assertResp(t, resp, &protostatus.Status{
			Code:    1024,
			Message: "session was not issued by the container owner",
		})
	})

	assertOK(t)
}

func TestService_Delete_SessionIssuer(t *testing.T) {
	ctx := context.Background()
	const currentEpoch = 10

	owner := usertest.User()
	id := cidtest.ID()
	var cnr container.Container
	cnr.SetOwner(owner.ID)

	otherID := cidtest.OtherID(id)
	var otherCnr container.Container
	otherCnr.SetOwner(usertest.OtherID(owner.ID))

	var fsChain testFSChain
	fsChain.epoch = currentEpoch
	fsChain.cnrs = map[cid.ID]container.Container{
		id:      cnr,
		otherID: otherCnr,
	}

	s := containerSvc.New(&owner.ECDSAPrivateKey, &fsChain, &fsChain, &fsChain, &fsChain)

	eACL := eacltest.Table()

	var st session.Container
	st.SetID(uuid.New())
	st.SetExp(fsChain.epoch + 1)
	st.SetAuthKey(neofscryptotest.Signer().Public())
	st.ForVerb(session.VerbContainerDelete)
	require.NoError(t, st.Sign(owner))

	call := func(t *testing.T, id cid.ID, signer usertest.UserSigner) *protocontainer.DeleteResponse {
		eACL.SetCID(id)

		req := makeDeleteRequestWithSession(t, owner, id, st)

		resp, err := s.Delete(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NoError(t, neofscrypto.VerifyResponseWithBuffer(resp, nil))

		resp.VerifyHeader = nil
		return resp
	}

	assertResp := func(t *testing.T, resp *protocontainer.DeleteResponse, st *protostatus.Status) {
		require.True(t, proto.Equal(resp, &protocontainer.DeleteResponse{
			MetaHeader: &protosession.ResponseMetaHeader{
				Version: version.Current().ProtoMessage(),
				Epoch:   currentEpoch,
				Status:  st,
			},
		}), resp.GetMetaHeader().GetStatus())
	}

	assertOK := func(t *testing.T) {
		resp := call(t, id, owner)
		assertResp(t, resp, nil)
	}

	t.Run("unavailable container", func(t *testing.T) {
		someError := errors.New("some error")
		fsChain.getErr = someError

		resp := call(t, id, owner)

		assertResp(t, resp, &protostatus.Status{
			Code:    1024,
			Message: someError.Error(),
		})

		fsChain.getErr = nil
	})

	assertOK(t)

	t.Run("not a container owner", func(t *testing.T) {
		resp := call(t, otherID, owner)

		assertResp(t, resp, &protostatus.Status{
			Code:    1024,
			Message: "session was not issued by the container owner",
		})
	})

	assertOK(t)
}
