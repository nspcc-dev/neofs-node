package crypto_test

import (
	"slices"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateContainerRequest(t *testing.T) {
	t.Run("signature mismatch", func(t *testing.T) {
		for i := range containerSig {
			cp := slices.Clone(containerSig)
			cp[i]++
			err := icrypto.AuthenticateContainerRequest(mainAcc, cp, mainAccECDSAPub, containerPayload, nil)
			require.EqualError(t, err, "signature mismatch")
		}
	})
	t.Run("owner mismatch", func(t *testing.T) {
		err := icrypto.AuthenticateContainerRequest(otherAcc, containerSig, mainAccECDSAPub, containerPayload, nil)
		require.EqualError(t, err, "owner mismatches signature")
	})

	err := icrypto.AuthenticateContainerRequest(mainAcc, containerSig, mainAccECDSAPub, containerPayload, nil)
	require.NoError(t, err)
}

var (
	containerPayload = []byte("container")
	// corresponds to mainAccECDSAPub.
	containerSig = []byte{174, 92, 87, 20, 94, 30, 25, 71, 87, 198, 157, 48, 83, 62, 218, 60, 56, 189, 14, 80, 210, 206, 165,
		241, 54, 178, 194, 111, 225, 69, 246, 163, 206, 209, 239, 4, 56, 128, 44, 218, 208, 91, 2, 125, 188, 216, 249, 64, 36,
		68, 121, 186, 21, 171, 62, 48, 251, 126, 59, 78, 94, 159, 114, 207}
)
