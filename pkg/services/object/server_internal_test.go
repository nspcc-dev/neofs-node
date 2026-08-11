package object

import (
	"testing"

	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
)

type mutuallyAuthenticatedClient bool

func (x mutuallyAuthenticatedClient) IsMutuallyAuthenticated() bool {
	return bool(x)
}

func TestShouldSignOutgoingRequest(t *testing.T) {
	requestWithTTL := func(ttl uint32) *protoobject.GetRequest {
		return &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: ttl}}
	}

	require.False(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), requestWithTTL(1)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), requestWithTTL(2)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(true), new(protoobject.GetRequest)))
	require.True(t, shouldSignOutgoingRequest(mutuallyAuthenticatedClient(false), requestWithTTL(1)))
	require.True(t, shouldSignOutgoingRequest(struct{}{}, requestWithTTL(1)))
}
