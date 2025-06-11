package util_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/stretchr/testify/require"
)

func TestVersionLE(t *testing.T) {
	req := new(protoobject.GetRequest) // any

	require.True(t, util.VersionLE(req, 2, 0))
	require.True(t, util.VersionLE(req, 0, 1))

	req.MetaHeader = &protosession.RequestMetaHeader{
		Version: &refs.Version{Major: 2, Minor: 21},
	}

	require.True(t, util.VersionLE(req, 2, 21))
	require.True(t, util.VersionLE(req, 2, 22))

	require.False(t, util.VersionLE(req, 2, 20))
	require.False(t, util.VersionLE(req, 1, 22))
}

func TestSignResponseIfNeeded(t *testing.T) {
	req := new(protoobject.GetRequest)
	resp := new(protoobject.GetResponse)
	key := neofscryptotest.Signer().ECDSAPrivateKey

	require.NotNil(t, util.SignResponseIfNeeded(&key, resp, req))

	req.MetaHeader = &protosession.RequestMetaHeader{
		Version: &refs.Version{Major: 2, Minor: 21},
	}
	require.NotNil(t, util.SignResponseIfNeeded(&key, resp, req))

	req.MetaHeader.Version.Minor = 22
	require.Nil(t, util.SignResponseIfNeeded(&key, resp, req))

	req.MetaHeader.Version.Major = 3
	req.MetaHeader.Version.Minor = 0
	require.Nil(t, util.SignResponseIfNeeded(&key, resp, req))
}
