package container

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type nopNetwork struct {
}

func (nopNetwork) CurrentEpoch() uint64 {
	return 0
}

type mockTimeProvider struct{}

func (mockTimeProvider) Now() time.Time {
	return time.Now()
}

type mockContainerContract struct {
	cnr container.Container
}

func (mockContainerContract) Put(context.Context, container.Container, []byte, []byte, []byte) (cid.ID, error) {
	panic("unimplemented")
}

func (x mockContainerContract) Get(cid.ID) (container.Container, error) {
	return x.cnr, nil
}

func (mockContainerContract) List(user.ID) ([]cid.ID, error) {
	panic("unimplemented")
}

func (mockContainerContract) PutEACL(context.Context, eacl.Table, []byte, []byte, []byte) error {
	panic("unimplemented")
}

func (mockContainerContract) GetEACL(cid.ID) (eacl.Table, error) {
	panic("unimplemented")
}

func (mockContainerContract) Delete(context.Context, cid.ID, []byte, []byte, []byte) error {
	panic("unimplemented")
}

func (mockContainerContract) SetAttribute(context.Context, cid.ID, string, string, uint64, []byte, []byte, []byte) error {
	panic("unimplemented")
}

func (mockContainerContract) RemoveAttribute(context.Context, cid.ID, string, uint64, []byte, []byte, []byte) error {
	panic("unimplemented")
}

func BenchmarkSessionTokenVerification(b *testing.B) {
	const anyVerb = session.VerbContainerPut
	anyCnrID := cidtest.ID()
	anyUsr := usertest.User()

	var cnr container.Container
	cnr.SetOwner(anyUsr.ID)

	var tok session.Container
	tok.SetID(uuid.New())
	tok.ForVerb(anyVerb)
	tok.SetExp(1)
	tok.SetAuthKey(anyUsr.Public())
	require.NoError(b, tok.Sign(anyUsr))

	metaHdr := &protosession.RequestMetaHeader{
		SessionToken: tok.ProtoMessage(),
	}

	s := New(&anyUsr.ECDSAPrivateKey, nopNetwork{}, nil, mockContainerContract{cnr: cnr}, nil, mockTimeProvider{})

	for b.Loop() {
		_, _, err := s.getVerifiedSessionTokenFromMetaHeader(metaHdr, anyVerb, anyCnrID)
		require.NoError(b, err)
		s.ResetSessionTokenCheckCache()
	}
}

func BenchmarkSessionTokenV2Verification(b *testing.B) {
	const anyVerbV2 = sessionv2.VerbContainerPut
	anyCnr := cidtest.ID()
	anyUsr := usertest.User()

	var cnr container.Container
	cnr.SetOwner(anyUsr.ID)

	var tok sessionv2.Token
	tok.SetVersion(sessionv2.TokenCurrentVersion)

	ctx, err := sessionv2.NewContext(anyCnr, []sessionv2.Verb{anyVerbV2})
	require.NoError(b, err)
	err = tok.AddContext(ctx)
	require.NoError(b, err)

	err = tok.AddSubject(sessionv2.NewTargetUser(anyUsr.UserID()))
	require.NoError(b, err)

	currentTime := time.Now()
	tok.SetIat(currentTime)
	tok.SetNbf(currentTime)
	tok.SetExp(currentTime.Add(1 * time.Hour))
	require.NoError(b, tok.Sign(anyUsr))

	metaHdr := &protosession.RequestMetaHeader{
		SessionTokenV2: tok.ProtoMessage(),
	}

	s := New(&anyUsr.ECDSAPrivateKey, nopNetwork{}, nil, mockContainerContract{cnr: cnr}, nil, mockTimeProvider{})

	for b.Loop() {
		_, _, err := s.getVerifiedSessionTokenV2FromMetaHeader(metaHdr, anyVerbV2, anyCnr)
		require.NoError(b, err)
		s.ResetSessionTokenCheckCache()
	}
}
