package v2

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	isessions "github.com/nspcc-dev/neofs-node/internal/sessions"
	nnscore "github.com/nspcc-dev/neofs-node/pkg/core/nns"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
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
		_, _, err = getCredentialsFromSessionV1Token(token, anyUsr.ID, anyUsr.PublicKeyBytes)
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
		_, _, err = s.getCredentialsFromSessionToken(token, anyUsr.ID)
		require.NoError(b, err)
		s.ResetTokenCheckCache()
	}
}

func TestGetRequestCredentialsSessionTokens(t *testing.T) {
	issuer, holder, attacker, delegator := usertest.User(), usertest.User(), usertest.User(), usertest.User()
	intermediate := usertest.User()
	cnr := cidtest.ID()

	var v1 session.Object
	v1.SetID(uuid.New())
	v1.ForVerb(session.VerbObjectGet)
	v1.BindContainer(cnr)
	v1.SetExp(1)
	v1.SetAuthKey(holder.Public())
	require.NoError(t, v1.Sign(issuer))

	ctx, err := sessionv2.NewContext(cnr, []sessionv2.Verb{sessionv2.VerbObjectGet})
	require.NoError(t, err)
	var v2 sessionv2.Token
	v2.SetVersion(sessionv2.TokenCurrentVersion)
	v2.SetIat(time.Now().Add(-time.Minute))
	v2.SetNbf(v2.Iat())
	v2.SetExp(v2.Iat().Add(time.Hour))
	require.NoError(t, v2.AddContext(ctx))
	require.NoError(t, v2.AddSubject(sessionv2.NewTargetUser(holder.ID)))
	require.NoError(t, v2.Sign(issuer))

	var origin, delegated sessionv2.Token
	v2.CopyTo(&origin)
	require.NoError(t, origin.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(delegator.ID)}))
	require.NoError(t, origin.Sign(issuer))
	v2.CopyTo(&delegated)
	delegated.SetOrigin(&origin)
	require.NoError(t, delegated.Sign(delegator))

	var final, middle, deeplyDelegated sessionv2.Token
	v2.CopyTo(&final)
	final.SetFinal(true)
	require.NoError(t, final.Sign(issuer))
	delegated.CopyTo(&middle)
	require.NoError(t, middle.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(intermediate.ID)}))
	require.NoError(t, middle.Sign(delegator))
	v2.CopyTo(&deeplyDelegated)
	deeplyDelegated.SetOrigin(&middle)
	require.NoError(t, deeplyDelegated.Sign(intermediate))

	for _, tc := range []struct {
		name           string
		tokens         common.RequestTokens
		requestIssuer  user.ID
		checkProtocols bool
	}{
		{name: "V1", tokens: common.RequestTokens{SessionV1: &v1}, requestIssuer: issuer.ID, checkProtocols: true},
		{name: "V2", tokens: common.RequestTokens{Session: &v2}, requestIssuer: issuer.ID, checkProtocols: true},
		{name: "V2 final", tokens: common.RequestTokens{Session: &final}, requestIssuer: issuer.ID},
		{name: "V2 delegated", tokens: common.RequestTokens{Session: &delegated}, requestIssuer: delegator.ID},
		{name: "V2 multi-level delegation", tokens: common.RequestTokens{Session: &deeplyDelegated}, requestIssuer: intermediate.ID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(nopFSChain{}, isessions.NewObjectSessionsCache(1),
				WithIRFetcher(nopIR{}), WithNetmapper(nopNetmapContract{}),
				WithContainerSource(nopContrainerContract{}), WithTimeProvider(mockChainTime{}))
			versions := []uint32{26}
			if tc.checkProtocols {
				versions = []uint32{24, 25, 26}
			}
			for _, minor := range versions {
				t.Run(fmt.Sprintf("API 2.%d", minor), func(t *testing.T) {
					authors := []struct {
						name string
						user usertest.UserSigner
					}{
						{name: "holder", user: holder},
						{name: "issuer", user: issuer},
						{name: "attacker", user: attacker},
						{name: "delegator", user: delegator},
						{name: "intermediate", user: intermediate},
					}
					if tc.requestIssuer == issuer.ID {
						authors = authors[:3]
					}
					for _, who := range authors {
						t.Run(who.name, func(t *testing.T) {
							allowed := who.user.ID == holder.ID || who.user.ID == tc.requestIssuer
							signers := []user.Signer{who.user.Signer}
							if tc.checkProtocols && minor == 26 {
								signers = append(signers, who.user.RFC6979, who.user.WalletConnect)
							}
							for _, signer := range signers {
								req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{
									Version: &refs.Version{Major: 2, Minor: minor},
								}}
								if tc.tokens.SessionV1 != nil {
									req.MetaHeader.SessionToken = tc.tokens.SessionV1.ProtoMessage()
								} else {
									req.MetaHeader.SessionTokenV2 = tc.tokens.Session.ProtoMessage()
								}
								var err error
								req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(signer, req, nil)
								require.NoError(t, err)
								require.NoError(t, icrypto.VerifyRequestSignatures(req))
								// API >=2.26 must ignore a spoofed BodySignature.
								if minor >= 26 && !allowed {
									req.VerifyHeader.BodySignature = &refs.Signature{Key: holder.PublicKeyBytes}
									require.NoError(t, icrypto.VerifyRequestSignatures(req))
								}
								// Cached token authentication must not bypass signer binding.
								tokens := tc.tokens
								if tokens.SessionV1 != nil {
									tok, err := s.VerifySessionV1TokenMessage(req.MetaHeader.SessionToken, session.VerbObjectGet, cnr, oid.ID{})
									require.NoError(t, err)
									tokens.SessionV1 = &tok
								} else {
									tok, err := s.VerifySessionTokenMessage(req.MetaHeader.SessionTokenV2, sessionv2.VerbObjectGet, cnr)
									require.NoError(t, err)
									tokens.Session = &tok
								}
								got, key, err := s.getRequestCredentials(context.Background(), req, tokens)
								if allowed {
									require.NoError(t, err)
									require.Equal(t, issuer.ID, got)
									require.Equal(t, issuer.PublicKeyBytes, key)
								} else {
									require.ErrorContains(t, err, "session token is not bound to the request signer")
								}
							}
						})
					}
				})
			}
			t.Run("missing verification header", func(t *testing.T) {
				_, _, err := s.getRequestCredentials(context.Background(), new(protoobject.GetRequest), tc.tokens)
				require.ErrorContains(t, err, "missing verification header")
			})
		})
	}
}

func TestGetRequestCredentialsN3SessionTokens(t *testing.T) {
	issuer, holder := usertest.User(), usertest.User()
	var v1 session.Object
	v1.SetAuthKey(holder.Public())
	require.NoError(t, v1.Sign(issuer))

	for _, script := range [][]byte{{0x11}, holder.PublicKeyBytes} {
		// Signature verification is performed before credential extraction.
		// In particular, even a script that decodes as an ECDSA public key
		// must retain its N3 identity rather than that ECDSA key's identity.
		req := &protoobject.GetRequest{
			MetaHeader: &protosession.RequestMetaHeader{Version: &refs.Version{Major: 2, Minor: 26}},
			VerifyHeader: &protosession.RequestVerificationHeader{RequestSignature: &refs.Signature{
				Scheme: refs.SignatureScheme_N3, Key: script,
			}},
		}
		var s Service
		_, _, err := s.getRequestCredentials(context.Background(), req, common.RequestTokens{SessionV1: &v1})
		require.ErrorContains(t, err, "requires an ECDSA-signed request")

		var v2 sessionv2.Token
		require.NoError(t, v2.AddSubject(sessionv2.NewTargetUser(user.NewFromScriptHash(hash.Hash160(script)))))
		require.NoError(t, v2.Sign(issuer))
		id, key, err := s.getRequestCredentials(context.Background(), req, common.RequestTokens{Session: &v2})
		require.NoError(t, err)
		require.Equal(t, issuer.ID, id)
		require.Equal(t, issuer.PublicKeyBytes, key)

		require.NoError(t, v2.SetSubjects([]sessionv2.Target{sessionv2.NewTargetUser(holder.ID)}))
		require.NoError(t, v2.Sign(issuer))
		_, _, err = s.getRequestCredentials(context.Background(), req, common.RequestTokens{Session: &v2})
		require.ErrorContains(t, err, "session token is not bound to the request signer")

		// The issuer exception is based on the authenticated account, not on
		// decoding its key as ECDSA. N3 issuers can use server-held ECDSA keys.
		n3Issuer := user.NewFromScriptHash(hash.Hash160(script))
		sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.N3, script, nil)
		v1.SetIssuer(n3Issuer)
		v1.AttachSignature(sig)
		id, key, err = s.getRequestCredentials(context.Background(), req, common.RequestTokens{SessionV1: &v1})
		require.NoError(t, err)
		require.Equal(t, n3Issuer, id)
		require.Equal(t, script, key)

		v2.SetIssuer(n3Issuer)
		v2.AttachSignature(sig)
		id, key, err = s.getRequestCredentials(context.Background(), req, common.RequestTokens{Session: &v2})
		require.NoError(t, err)
		require.Equal(t, n3Issuer, id)
		require.Equal(t, script, key)

		// Restore the ECDSA issuer for the next script's rejection case.
		require.NoError(t, v1.Sign(issuer))
	}
}

type sessionNNSChain struct {
	nopFSChain
	hasUser func(string, util.Uint160) (bool, error)
}

func (c sessionNNSChain) HasUserInNNS(name string, id util.Uint160) (bool, error) {
	return c.hasUser(name, id)
}

func TestGetRequestCredentialsSessionTokenNNS(t *testing.T) {
	issuer, holder := usertest.User(), usertest.User()
	var token sessionv2.Token
	require.NoError(t, token.AddSubject(sessionv2.NewTargetNamed("holder.neofs")))
	require.NoError(t, token.Sign(issuer))

	req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{
		Version: &refs.Version{Major: 2, Minor: 26}, SessionTokenV2: token.ProtoMessage(),
	}}
	var err error
	req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(holder, req, nil)
	require.NoError(t, err)
	require.NoError(t, icrypto.VerifyRequestSignatures(req))

	resolverErr := errors.New("NNS unavailable")
	for _, tc := range []struct {
		name    string
		allowed bool
		err     error
	}{
		{name: "authorized", allowed: true},
		{name: "unauthorized"},
		{name: "resolver failure", err: resolverErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Service{r: nnscore.NewResolver(sessionNNSChain{
				hasUser: func(name string, id util.Uint160) (bool, error) {
					require.Equal(t, "holder.neofs", name)
					require.Equal(t, holder.ID.ScriptHash(), id)
					return tc.allowed, tc.err
				},
			})}
			id, key, err := s.getRequestCredentials(context.Background(), req, common.RequestTokens{Session: &token})
			switch {
			case tc.err != nil:
				require.ErrorIs(t, err, resolverErr)
			case !tc.allowed:
				require.ErrorContains(t, err, "session token is not bound to the request signer")
			default:
				require.NoError(t, err)
				require.Equal(t, issuer.ID, id)
				require.Equal(t, issuer.PublicKeyBytes, key)
			}
		})
	}
}

func TestGetCredentialsFromPeerPublicKey(t *testing.T) {
	key, err := keys.NewPrivateKey()
	require.NoError(t, err)

	usr, actualPub, err := getCredentialsFromPeerPublicKey(key.PublicKey())
	require.NoError(t, err)
	require.Equal(t, user.NewFromECDSAPublicKey(key.PrivateKey.PublicKey), usr)
	require.Equal(t, key.PublicKey().Bytes(), actualPub)
}

func TestGetRequestCredentialsUsesTLSAuthenticatedPeer(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	info, err := peerauth.NewAuthInfo(credentials.TLSInfo{State: tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{PublicKey: &key.PublicKey}},
	}})
	require.NoError(t, err)
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
	req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: 1}}

	var s Service
	for _, tokens := range []common.RequestTokens{
		{}, {Session: new(sessionv2.Token)}, {SessionV1: new(session.Object)},
	} {
		actualUser, actualPub, err := s.getRequestCredentials(ctx, req, tokens)
		require.NoError(t, err)
		require.Equal(t, user.NewFromECDSAPublicKey(key.PublicKey), actualUser)
		require.Equal(t, (*keys.PublicKey)(&key.PublicKey).Bytes(), actualPub)
	}

	t.Run("unsigned requests require authenticated TLS and TTL one", func(t *testing.T) {
		_, _, err := s.getRequestCredentials(peer.NewContext(ctx, new(peer.Peer)), req, common.RequestTokens{})
		require.ErrorContains(t, err, "missing verification header")
		for _, ttl := range []uint32{0, 2} {
			req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: ttl}}
			_, _, err := s.getRequestCredentials(ctx, req, common.RequestTokens{})
			require.ErrorContains(t, err, "missing verification header")
		}
	})

	t.Run("signed requests still require token binding", func(t *testing.T) {
		holder := user.NewAutoIDSigner(*key)
		var token session.Object
		token.SetAuthKey(holder.Public())
		require.NoError(t, token.Sign(holder))
		req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{
			Version: &refs.Version{Major: 2, Minor: 26}, Ttl: 1, SessionToken: token.ProtoMessage(),
		}}
		req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(usertest.User(), req, nil)
		require.NoError(t, err)
		require.NoError(t, icrypto.VerifyRequestSignatures(req))
		_, _, err = s.getRequestCredentials(ctx, req, common.RequestTokens{SessionV1: &token})
		require.ErrorContains(t, err, "session token is not bound to the request signer")
	})
}
