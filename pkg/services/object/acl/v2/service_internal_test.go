package v2

import (
	"testing"
	"time"

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
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type nopNetmapContract struct{}

func (c nopNetmapContract) GetEpochBlockByTime(uint32) (uint32, error) {
	panic("unimplemented")
}

type mockChainTime struct{}

func (mockChainTime) Now() time.Time {
	return time.Now()
}

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
	const anyVerbV2 = sessionv2.VerbObjectGet
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

	s := New(nopFSChain{},
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		_, _, err := s.getVerifiedSessionToken(metaHdr, anyVerb, anyVerbV2, anyCnr, oid.ID{})
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

	s := New(nopFSChain{},
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		_, err := s.getVerifiedBearerToken(metaHdr, anyCnr, anyCnrOwner.UserID(), anyReqSender)
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}

func BenchmarkSessionTokenV2Verification(b *testing.B) {
	const anyVerb = session.VerbObjectGet
	const anyVerbV2 = sessionv2.VerbObjectGet
	anyCnr := cidtest.ID()
	anyUsr := usertest.User()

	var tok sessionv2.Token
	tok.SetVersion(sessionv2.TokenCurrentVersion)
	tok.SetNonce(sessionv2.RandomNonce())

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

	s := New(nopFSChain{},
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		_, _, err := s.getVerifiedSessionToken(metaHdr, anyVerb, anyVerbV2, anyCnr, oid.ID{})
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}
