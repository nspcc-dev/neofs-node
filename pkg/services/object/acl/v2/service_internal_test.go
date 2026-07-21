package v2

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	isessions "github.com/nspcc-dev/neofs-node/internal/sessions"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
	anyCnr := cidtest.ID()
	anyUsr := usertest.User()

	var tok session.Object
	tok.SetID(uuid.New())
	tok.ForVerb(anyVerb)
	tok.BindContainer(anyCnr)
	tok.SetExp(1)
	tok.SetAuthKey(anyUsr.Public())
	require.NoError(b, tok.Sign(anyUsr))

	msg := tok.ProtoMessage()

	s := New(nopFSChain{},
		isessions.NewObjectSessionsCache(1),
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		token, err := s.VerifySessionV1TokenMessage(msg, anyVerb, anyCnr, oid.ID{})
		require.NoError(b, err)
		_, _, err = getCredentialsFromSessionV1Token(token)
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

	msg := tok.ProtoMessage()

	s := New(nopFSChain{},
		isessions.NewObjectSessionsCache(1),
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		token, err := s.VerifyBearerTokenMessage(msg)
		require.NoError(b, err)
		err = s.verifyBearerTokenAgainstRequest(token, anyCnr, anyCnrOwner.UserID(), anyReqSender)
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}

func BenchmarkSessionTokenV2Verification(b *testing.B) {
	const anyVerb = sessionv2.VerbObjectGet
	anyCnr := cidtest.ID()
	anyUsr := usertest.User()

	var tok sessionv2.Token
	tok.SetVersion(sessionv2.TokenCurrentVersion)

	ctx, err := sessionv2.NewContext(anyCnr, []sessionv2.Verb{anyVerb})
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

	msg := tok.ProtoMessage()

	s := New(nopFSChain{},
		isessions.NewObjectSessionsCache(1),
		WithIRFetcher(nopIR{}),
		WithNetmapper(nopNetmapContract{}),
		WithContainerSource(nopContrainerContract{}),
		WithTimeProvider(mockChainTime{}),
	)

	for b.Loop() {
		token, err := s.VerifySessionTokenMessage(msg, anyVerb, anyCnr)
		require.NoError(b, err)
		_, _, err = getCredentialsFromSessionToken(token)
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}

func TestGetCredentialsFromPeerPublicKey(t *testing.T) {
	key, err := keys.NewPrivateKey()
	require.NoError(t, err)
	pub := key.PublicKey().Bytes()

	usr, actualPub, err := getCredentialsFromPeerPublicKey(pub)
	require.NoError(t, err)
	require.Equal(t, user.NewFromECDSAPublicKey(key.PrivateKey.PublicKey), usr)
	require.Equal(t, pub, actualPub)

	_, _, err = getCredentialsFromPeerPublicKey([]byte("invalid"))
	require.ErrorContains(t, err, "invalid peer public key")
}

func TestGetRequestCredentialsPeerPriority(t *testing.T) {
	key, err := keys.NewPrivateKey()
	require.NoError(t, err)
	pub := key.PublicKey().Bytes()
	expectedUser := user.NewFromECDSAPublicKey(key.PrivateKey.PublicKey)

	for name, tokens := range map[string]common.RequestTokens{
		"V1 session": {
			SessionV1:                  new(session.Object),
			AuthenticatedPeerPublicKey: pub,
		},
		"V2 session": {
			Session:                    new(sessionv2.Token),
			AuthenticatedPeerPublicKey: pub,
		},
	} {
		t.Run(name, func(t *testing.T) {
			actualUser, actualPub, err := getRequestCredentials(tokens, nil)
			require.NoError(t, err)
			require.Equal(t, expectedUser, actualUser)
			require.Equal(t, pub, actualPub)
		})
	}
}
