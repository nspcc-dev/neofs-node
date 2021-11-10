package version_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestIsValid(t *testing.T) {
	require.True(t, version.IsValid(*versionSDK.Current()))

	var v versionSDK.Version

	for _, item := range []struct {
		mjr, mnr uint32
		valid    bool
	}{
		{mjr: 0, mnr: 0, valid: false},
		{mjr: 2, mnr: 6, valid: false},
		{mjr: 2, mnr: 7, valid: true},
		{mjr: 3, mnr: 0, valid: true},
	} {
		v.SetMajor(item.mjr)
		v.SetMinor(item.mnr)

		require.Equal(t, item.valid, version.IsValid(v), item)
	}
}
