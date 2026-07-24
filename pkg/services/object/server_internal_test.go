package object

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestRequestNeedsSignature(t *testing.T) {
	ctx, expectedKey := tlsPeerContext(t)

	require.True(t, requestNeedsSignature(context.Background(), requestWithTTL(1)))
	require.True(t, requestNeedsSignature(ctx, requestWithTTL(2)))
	require.True(t, requestNeedsSignature(ctx, new(protoobject.GetRequest)))
	require.False(t, requestNeedsSignature(ctx, requestWithTTL(1)))

	signedReq := requestWithTTL(1)
	signedReq.VerifyHeader = new(protosession.RequestVerificationHeader)
	require.True(t, requestNeedsSignature(ctx, signedReq))

	tokens := requestTokensWithPeer(ctx, requestWithTTL(1), common.RequestTokens{})
	require.Equal(t, expectedKey, tokens.AuthenticatedPeerPublicKey)
	tokens = requestTokensWithPeer(ctx, requestWithTTL(2), common.RequestTokens{})
	require.Nil(t, tokens.AuthenticatedPeerPublicKey)
	tokens = requestTokensWithPeer(ctx, signedReq, common.RequestTokens{})
	require.Nil(t, tokens.AuthenticatedPeerPublicKey)
}

func requestWithTTL(ttl uint32) *protoobject.GetRequest {
	return &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: ttl}}
}

func tlsPeerContext(t *testing.T) (context.Context, []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{PublicKey: &key.PublicKey}
	pub, err := peerauth.CertificatePublicKey(cert)
	require.NoError(t, err)
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}},
	})
	return ctx, pub
}
