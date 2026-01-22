package v2

import (
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type nopNetmapContract struct{}

func (nopNetmapContract) GetNetMapByEpoch(uint64) (*netmap.NetMap, error) {
	panic("unimplemented")
}

func (nopNetmapContract) Epoch() (uint64, error) {
	return 0, nil
}

func (nopNetmapContract) NetMap() (*netmap.NetMap, error) {
	panic("unimplemented")
}

func (nopNetmapContract) ServerInContainer(cid.ID) (bool, error) {
	panic("unimplemented")
}

func (nopNetmapContract) GetEpochBlock(uint64) (uint32, error) {
	panic("unimplemented")
}

type nopContrainerContract struct{}

func (nopContrainerContract) Get(cid.ID) (container.Container, error) {
	panic("unimplemented")
}

func BenchmarkSessionTokenVerification(b *testing.B) {
	const anyVerb = session.VerbObjectGet
	anyCnr := cidtest.ID()
	anyUsr := usertest.User()

	var tok session.Object
	tok.SetID(uuid.New())
	tok.ForVerb(anyVerb)
	tok.BindContainer(anyCnr)
	tok.SetExp(1)
	tok.SetAuthKey(anyUsr.Public())
	require.NoError(b, tok.Sign(anyUsr))

	metaHdr := &protosession.RequestMetaHeader{
		SessionToken: tok.ProtoMessage(),
	}

	s := New(nopFSChain{}, WithIRFetcher(nopIR{}), WithNetmapper(nopNetmapContract{}), WithContainerSource(nopContrainerContract{}))

	for b.Loop() {
		_, err := s.getVerifiedSessionToken(metaHdr, anyVerb, anyCnr, oid.ID{})
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}

func BenchmarkBearerTokenVerification(b *testing.B) {
	anyCnr := cidtest.ID()
	anyCnrOwner := usertest.User()
	anyReqSender := usertest.ID()
	anyEACL := eacl.Table{}

	var tok bearer.Token
	tok.SetEACLTable(anyEACL)
	tok.SetExp(1)
	require.NoError(b, tok.Sign(anyCnrOwner))

	metaHdr := &protosession.RequestMetaHeader{
		BearerToken: tok.ProtoMessage(),
	}

	s := New(nopFSChain{}, WithIRFetcher(nopIR{}), WithNetmapper(nopNetmapContract{}), WithContainerSource(nopContrainerContract{}))

	for b.Loop() {
		_, err := s.getVerifiedBearerToken(metaHdr, anyCnr, anyCnrOwner.UserID(), anyReqSender)
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}
