package container

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type nopNetwork struct {
}

func (nopNetwork) CurrentEpoch() uint64 {
	return 0
}

type mockContainerContract struct {
	cnr container.Container
}

func (mockContainerContract) Put(context.Context, container.Container, []byte, []byte, *session.Container) (cid.ID, error) {
	panic("unimplemented")
}

func (x mockContainerContract) Get(cid.ID) (container.Container, error) {
	return x.cnr, nil
}

func (mockContainerContract) List(user.ID) ([]cid.ID, error) {
	panic("unimplemented")
}

func (mockContainerContract) PutEACL(context.Context, eacl.Table, []byte, []byte, *session.Container) error {
	panic("unimplemented")
}

func (mockContainerContract) GetEACL(cid.ID) (eacl.Table, error) {
	panic("unimplemented")
}

func (mockContainerContract) Delete(context.Context, cid.ID, []byte, []byte, *session.Container) error {
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

	s := New(&anyUsr.ECDSAPrivateKey, nopNetwork{}, nil, mockContainerContract{cnr: cnr}, nil)

	for b.Loop() {
		_, err := s.getVerifiedSessionToken(metaHdr, anyVerb, anyCnrID)
		require.NoError(b, err)
		s.ResetSessionTokenCheckCache()
	}
}
