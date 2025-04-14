package crypto_test

import (
	"slices"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateContainerRequest(t *testing.T) {
	t.Run("invalid public key", func(t *testing.T) {
		for _, tc := range []struct {
			name, err string
			changePub func([]byte) []byte
		}{
			{name: "nil", err: "decode public key: EOF", changePub: func([]byte) []byte { return nil }},
			{name: "empty", err: "decode public key: EOF", changePub: func([]byte) []byte { return []byte{} }},
			{name: "undersize", err: "decode public key: unexpected EOF", changePub: func(k []byte) []byte { return k[:len(k)-1] }},
			{name: "oversize", err: "decode public key: extra data", changePub: func(k []byte) []byte { return append(k, 1) }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := icrypto.AuthenticateContainerRequestRFC6979(mainAcc, tc.changePub(mainAccECDSAPub), containerSig, containerPayload)
				require.EqualError(t, err, tc.err)
			})
		}
	})
	t.Run("signature mismatch", func(t *testing.T) {
		for i := range containerSig {
			cp := slices.Clone(containerSig)
			cp[i]++
			err := icrypto.AuthenticateContainerRequestRFC6979(mainAcc, mainAccECDSAPub, cp, containerPayload)
			require.EqualError(t, err, "signature mismatch")
		}
	})
	t.Run("owner mismatch", func(t *testing.T) {
		err := icrypto.AuthenticateContainerRequestRFC6979(otherAcc, mainAccECDSAPub, containerSig, containerPayload)
		require.EqualError(t, err, "owner mismatches signature")
	})

	err := icrypto.AuthenticateContainerRequestRFC6979(mainAcc, mainAccECDSAPub, containerSig, containerPayload)
	require.NoError(t, err)
}

var (
	containerPayload = []byte("container")
	// corresponds to mainAccECDSAPub.
	containerSig = []byte{174, 92, 87, 20, 94, 30, 25, 71, 87, 198, 157, 48, 83, 62, 218, 60, 56, 189, 14, 80, 210, 206, 165,
		241, 54, 178, 194, 111, 225, 69, 246, 163, 206, 209, 239, 4, 56, 128, 44, 218, 208, 91, 2, 125, 188, 216, 249, 64, 36,
		68, 121, 186, 21, 171, 62, 48, 251, 126, 59, 78, 94, 159, 114, 207}
)
