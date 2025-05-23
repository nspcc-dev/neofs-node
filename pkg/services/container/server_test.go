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
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type testFSChain struct {
	cnr   container.Container
	epoch uint64
}

func (x testFSChain) CurrentEpoch() uint64 { return x.epoch }

func (testFSChain) Put(container.Container, []byte, []byte, *session.Container) (cid.ID, error) {
	return cid.ID{}, errors.New("unimplemented")
}

func (x testFSChain) Get(cid.ID) (container.Container, error) {
	return x.cnr, nil
}

func (testFSChain) List(user.ID) ([]cid.ID, error) {
	return nil, errors.New("unimplemented")
}

func (testFSChain) PutEACL(eacl.Table, []byte, []byte, *session.Container) error {
	return errors.New("unimplemented")
}

func (testFSChain) GetEACL(cid.ID) (eacl.Table, error) {
	return eacl.Table{}, errors.New("unimplemented")
}

func (testFSChain) Delete(cid.ID, []byte, []byte, *session.Container) error {
	return errors.New("unimplemented")
}

func (x testFSChain) GetEpochBlock(uint64) (uint32, error) { panic("unimplemented") }

func (x testFSChain) InvokeContainedScript(*transaction.Transaction, *block.Header, *trigger.Type, *bool) (*result.Invoke, error) {
	panic("unimplemented")
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

	const anyEpoch = 10
	var cnr container.Container
	cnr.SetOwner(usr.ID)

	m := &testFSChain{
		cnr:   cnr,
		epoch: anyEpoch,
	}
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
				require.Equal(t, "wrong container session operation: PUT", sts.Message)
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

				req := makeDeleteRequestWithSession(t, usr, cidtest.ID(), st)
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
