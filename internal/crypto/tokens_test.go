package crypto_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/google/uuid"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateToken_Bearer(t *testing.T) {
	t.Run("without signature", func(t *testing.T) {
		token := getUnsignedBearerToken()
		require.EqualError(t, icrypto.AuthenticateToken(&token), "missing signature")
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		token := bearerECDSASHA512
		sig, _ := token.Signature()
		sig.SetScheme(3)
		token.AttachSignature(sig)
		require.EqualError(t, icrypto.AuthenticateToken(&token), "unsupported scheme 3")
	})
	t.Run("invalid public key", func(t *testing.T) {
		for _, tc := range []struct {
			name, err string
			changePub func([]byte) []byte
		}{
			{name: "nil", err: "EOF", changePub: func([]byte) []byte { return nil }},
			{name: "empty", err: "EOF", changePub: func([]byte) []byte { return []byte{} }},
			{name: "undersize", err: "unexpected EOF", changePub: func(k []byte) []byte { return k[:len(k)-1] }},
			{name: "oversize", err: "extra data", changePub: func(k []byte) []byte { return append(k, 1) }},
			{name: "prefix 0", err: "invalid prefix 0", changePub: func(k []byte) []byte { return []byte{0x00} }},
			{name: "prefix 1", err: "invalid prefix 1", changePub: func(k []byte) []byte { return []byte{0x01} }},
			{name: "prefix 4", err: "EOF", changePub: func(k []byte) []byte { return []byte{0x04} }},
			{name: "prefix 5", err: "invalid prefix 5", changePub: func(k []byte) []byte { return []byte{0x05} }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				token := bearerECDSASHA512
				sig, _ := token.Signature()
				sig.SetPublicKeyBytes(tc.changePub(sig.PublicKeyBytes()))
				token.AttachSignature(sig)
				err := icrypto.AuthenticateToken(&token)
				require.EqualError(t, err, "scheme ECDSA_SHA512: decode public key: "+tc.err)
			})
		}
	})
	t.Run("signature mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  bearer.Token
		}{
			{neofscrypto.ECDSA_SHA512, bearerECDSASHA512},
			{neofscrypto.ECDSA_DETERMINISTIC_SHA256, bearerECDSARFC6979},
			{neofscrypto.ECDSA_WALLETCONNECT, bearerECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				sig, _ := tc.token.Signature()
				validSig := sig.Value()
				for i := range validSig {
					cp := slices.Clone(validSig)
					cp[i]++
					sig.SetValue(cp)
					tc.token.AttachSignature(sig)
					err := icrypto.AuthenticateToken(&tc.token)
					require.EqualError(t, err, fmt.Sprintf("scheme %s: signature mismatch", tc.scheme))
				}
			})
		}
	})
	t.Run("without issuer", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  bearer.Token
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: noIssuerBearerECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: noIssuerBearerECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: noIssuerBearerECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "missing issuer")
			})
		}
	})
	t.Run("issuer mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  bearer.Token
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: wrongIssuerBearerECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: wrongIssuerBearerECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: wrongIssuerBearerECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "issuer mismatches signature")
			})
		}
	})
	for _, tc := range []struct {
		scheme neofscrypto.Scheme
		token  bearer.Token
	}{
		{scheme: neofscrypto.ECDSA_SHA512, token: bearerECDSASHA512},
		{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: bearerECDSARFC6979},
		{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: bearerECDSAWalletConnect},
	} {
		require.NoError(t, icrypto.AuthenticateToken(&tc.token))
	}
}

func TestAuthenticateSessionToken_Object(t *testing.T) {
	t.Run("without signature", func(t *testing.T) {
		token := getUnsignedObjectSessionToken()
		require.EqualError(t, icrypto.AuthenticateToken(&token), "missing signature")
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		token := objectSessionECDSASHA512
		sig, _ := token.Signature()
		sig.SetScheme(3)
		token.AttachSignature(sig)
		require.EqualError(t, icrypto.AuthenticateToken(&token), "unsupported scheme 3")
	})
	t.Run("invalid public key", func(t *testing.T) {
		for _, tc := range []struct {
			name, err string
			changePub func([]byte) []byte
		}{
			{name: "nil", err: "EOF", changePub: func([]byte) []byte { return nil }},
			{name: "empty", err: "EOF", changePub: func([]byte) []byte { return []byte{} }},
			{name: "undersize", err: "unexpected EOF", changePub: func(k []byte) []byte { return k[:len(k)-1] }},
			{name: "oversize", err: "extra data", changePub: func(k []byte) []byte { return append(k, 1) }},
			{name: "prefix 0", err: "invalid prefix 0", changePub: func(k []byte) []byte { return []byte{0x00} }},
			{name: "prefix 1", err: "invalid prefix 1", changePub: func(k []byte) []byte { return []byte{0x01} }},
			{name: "prefix 4", err: "EOF", changePub: func(k []byte) []byte { return []byte{0x04} }},
			{name: "prefix 5", err: "invalid prefix 5", changePub: func(k []byte) []byte { return []byte{0x05} }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				token := objectSessionECDSASHA512
				sig, _ := token.Signature()
				sig.SetPublicKeyBytes(tc.changePub(sig.PublicKeyBytes()))
				token.AttachSignature(sig)
				err := icrypto.AuthenticateToken(&token)
				require.EqualError(t, err, "scheme ECDSA_SHA512: decode public key: "+tc.err)
			})
		}
	})
	t.Run("signature mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Object
		}{
			{neofscrypto.ECDSA_SHA512, objectSessionECDSASHA512},
			{neofscrypto.ECDSA_DETERMINISTIC_SHA256, objectSessionECDSARFC6979},
			{neofscrypto.ECDSA_WALLETCONNECT, objectSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				sig, _ := tc.token.Signature()
				validSig := sig.Value()
				for i := range validSig {
					cp := slices.Clone(validSig)
					cp[i]++
					sig.SetValue(cp)
					tc.token.AttachSignature(sig)
					err := icrypto.AuthenticateToken(&tc.token)
					require.EqualError(t, err, fmt.Sprintf("scheme %s: signature mismatch", tc.scheme))
				}
			})
		}
	})
	t.Run("without issuer", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Object
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: noIssuerObjectSessionECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: noIssuerObjectSessionECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: noIssuerObjectSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "missing issuer")
			})
		}
	})
	t.Run("issuer mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Object
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: wrongIssuerObjectSessionECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: wrongIssuerObjectSessionECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: wrongIssuerObjectSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "issuer mismatches signature")
			})
		}
	})
	for _, tc := range []struct {
		scheme neofscrypto.Scheme
		token  session.Object
	}{
		{scheme: neofscrypto.ECDSA_SHA512, token: objectSessionECDSASHA512},
		{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: objectSessionECDSARFC6979},
		{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: objectSessionECDSAWalletConnect},
	} {
		require.NoError(t, icrypto.AuthenticateToken(&tc.token))
	}
}

func TestAuthenticateSessionToken_Container(t *testing.T) {
	t.Run("without signature", func(t *testing.T) {
		token := getUnsignedContainerSessionToken()
		require.EqualError(t, icrypto.AuthenticateToken(&token), "missing signature")
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		token := containerSessionECDSASHA512
		sig, _ := token.Signature()
		sig.SetScheme(3)
		token.AttachSignature(sig)
		require.EqualError(t, icrypto.AuthenticateToken(&token), "unsupported scheme 3")
	})
	t.Run("invalid public key", func(t *testing.T) {
		for _, tc := range []struct {
			name, err string
			changePub func([]byte) []byte
		}{
			{name: "nil", err: "EOF", changePub: func([]byte) []byte { return nil }},
			{name: "empty", err: "EOF", changePub: func([]byte) []byte { return []byte{} }},
			{name: "undersize", err: "unexpected EOF", changePub: func(k []byte) []byte { return k[:len(k)-1] }},
			{name: "oversize", err: "extra data", changePub: func(k []byte) []byte { return append(k, 1) }},
			{name: "prefix 0", err: "invalid prefix 0", changePub: func(k []byte) []byte { return []byte{0x00} }},
			{name: "prefix 1", err: "invalid prefix 1", changePub: func(k []byte) []byte { return []byte{0x01} }},
			{name: "prefix 4", err: "EOF", changePub: func(k []byte) []byte { return []byte{0x04} }},
			{name: "prefix 5", err: "invalid prefix 5", changePub: func(k []byte) []byte { return []byte{0x05} }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				token := containerSessionECDSASHA512
				sig, _ := token.Signature()
				sig.SetPublicKeyBytes(tc.changePub(sig.PublicKeyBytes()))
				token.AttachSignature(sig)
				err := icrypto.AuthenticateToken(&token)
				require.EqualError(t, err, "scheme ECDSA_SHA512: decode public key: "+tc.err)
			})
		}
	})
	t.Run("signature mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Container
		}{
			{neofscrypto.ECDSA_SHA512, containerSessionECDSASHA512},
			{neofscrypto.ECDSA_DETERMINISTIC_SHA256, containerSessionECDSARFC6979},
			{neofscrypto.ECDSA_WALLETCONNECT, containerSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				sig, _ := tc.token.Signature()
				validSig := sig.Value()
				for i := range validSig {
					cp := slices.Clone(validSig)
					cp[i]++
					sig.SetValue(cp)
					tc.token.AttachSignature(sig)
					err := icrypto.AuthenticateToken(&tc.token)
					require.EqualError(t, err, fmt.Sprintf("scheme %s: signature mismatch", tc.scheme))
				}
			})
		}
	})
	t.Run("without issuer", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Container
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: noIssuerContainerSessionECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: noIssuerContainerSessionECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: noIssuerContainerSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "missing issuer")
			})
		}
	})
	t.Run("issuer mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			token  session.Container
		}{
			{scheme: neofscrypto.ECDSA_SHA512, token: wrongIssuerContainerSessionECDSASHA512},
			{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: wrongIssuerContainerSessionECDSARFC6979},
			{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: wrongIssuerContainerSessionECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				require.EqualError(t, icrypto.AuthenticateToken(&tc.token), "issuer mismatches signature")
			})
		}
	})
	for _, tc := range []struct {
		scheme neofscrypto.Scheme
		token  session.Container
	}{
		{scheme: neofscrypto.ECDSA_SHA512, token: containerSessionECDSASHA512},
		{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, token: containerSessionECDSARFC6979},
		{scheme: neofscrypto.ECDSA_WALLETCONNECT, token: containerSessionECDSAWalletConnect},
	} {
		require.NoError(t, icrypto.AuthenticateToken(&tc.token))
	}
}

var (
	// ECDSA private key used in tests.
	// privECDSA = ecdsa.PrivateKey{
	// 	PublicKey: ecdsa.PublicKey{
	// 		Curve: elliptic.P256(),
	// 		X: new(big.Int).SetBytes([]byte{206, 67, 193, 231, 254, 180, 127, 78, 101, 154, 23, 161, 134, 77, 122, 34, 234, 85,
	// 			149, 44, 32, 223, 244, 140, 28, 194, 76, 214, 239, 121, 174, 40}),
	// 		Y: new(big.Int).SetBytes([]byte{170, 190, 155, 176, 31, 11, 4, 14, 103, 210, 53, 0, 73, 46, 81, 129, 163, 217, 81, 51, 111,
	// 			135, 223, 253, 48, 104, 240, 197, 122, 37, 197, 78}),
	// 	},
	// 	D: new(big.Int).SetBytes([]byte{185, 97, 226, 151, 175, 3, 234, 11, 168, 211, 53, 141, 136, 102, 100, 222, 73, 174, 234, 157,
	// 		139, 192, 66, 145, 13, 173, 12, 120, 22, 134, 52, 180}),
	// }
	// corresponds to the private key.
	pubECDSA = []byte{2, 206, 67, 193, 231, 254, 180, 127, 78, 101, 154, 23, 161, 134, 77, 122, 34, 234, 85, 149, 44, 32, 223,
		244, 140, 28, 194, 76, 214, 239, 121, 174, 40}
	// corresponds to pubECDSA.
	issuer = user.ID{53, 57, 243, 96, 136, 255, 217, 227, 204, 13, 243, 228, 109, 31, 226, 226, 236, 62, 13, 190, 156, 135, 252, 236, 8}
)

// set in init.
var (
	bearerECDSASHA512        bearer.Token
	bearerECDSARFC6979       bearer.Token
	bearerECDSAWalletConnect bearer.Token

	noIssuerBearerECDSASHA512        bearer.Token
	noIssuerBearerECDSARFC6979       bearer.Token
	noIssuerBearerECDSAWalletConnect bearer.Token

	wrongIssuerBearerECDSASHA512        bearer.Token
	wrongIssuerBearerECDSARFC6979       bearer.Token
	wrongIssuerBearerECDSAWalletConnect bearer.Token

	objectSessionECDSASHA512        session.Object
	objectSessionECDSARFC6979       session.Object
	objectSessionECDSAWalletConnect session.Object

	noIssuerObjectSessionECDSASHA512        session.Object
	noIssuerObjectSessionECDSARFC6979       session.Object
	noIssuerObjectSessionECDSAWalletConnect session.Object

	wrongIssuerObjectSessionECDSASHA512        session.Object
	wrongIssuerObjectSessionECDSARFC6979       session.Object
	wrongIssuerObjectSessionECDSAWalletConnect session.Object

	containerSessionECDSASHA512        session.Container
	containerSessionECDSARFC6979       session.Container
	containerSessionECDSAWalletConnect session.Container

	noIssuerContainerSessionECDSASHA512        session.Container
	noIssuerContainerSessionECDSARFC6979       session.Container
	noIssuerContainerSessionECDSAWalletConnect session.Container

	wrongIssuerContainerSessionECDSASHA512        session.Container
	wrongIssuerContainerSessionECDSARFC6979       session.Container
	wrongIssuerContainerSessionECDSAWalletConnect session.Container
)

func getUnsignedBearerToken() bearer.Token {
	token := getUnsignedNoIssuerBearerToken()
	token.SetIssuer(issuer)
	return token
}

func getUnsignedNoIssuerBearerToken() bearer.Token {
	cnr := cid.ID{61, 208, 16, 128, 106, 78, 90, 196, 156, 65, 180, 142, 62, 137, 245, 242, 69, 250, 212, 176, 35, 114, 239, 114, 53,
		231, 19, 14, 46, 67, 163, 155}
	rs := []eacl.Record{
		eacl.ConstructRecord(1358410124, 2013986849, []eacl.Target{
			eacl.NewTargetByRole(1744308170),
			eacl.NewTargetByAccounts([]user.ID{
				{53, 235, 71, 238, 229, 61, 147, 96, 70, 61, 208, 119, 180, 143, 97, 251, 227, 9, 123, 1, 221, 188, 110, 23, 2},
				{53, 199, 135, 136, 133, 147, 241, 4, 87, 218, 54, 148, 163, 152, 212, 136, 112, 212, 213, 82, 129, 12, 249, 191, 196},
			}),
		},
			eacl.ConstructFilter(316417641, "1889646963", 697414400, "1523651353"),
			eacl.ConstructFilter(1569143202, "1132155987", 1317877644, "761720708"),
		),
		eacl.ConstructRecord(435879936, 750558462, []eacl.Target{
			eacl.NewTargetByRole(821801204),
			eacl.NewTargetByAccounts([]user.ID{
				{53, 248, 103, 213, 205, 251, 58, 107, 227, 204, 198, 118, 95, 145, 250, 210, 173, 233, 217, 68, 147, 17, 78, 43, 175},
				{53, 227, 67, 50, 13, 210, 101, 123, 157, 69, 19, 114, 119, 142, 213, 242, 36, 7, 238, 75, 196, 109, 51, 39, 254},
			}),
		},
			eacl.ConstructFilter(2142014873, "752700729", 1812438022, "1131635574"),
			eacl.ConstructFilter(483205511, "820191356", 1929251403, "763318049"),
		),
	}

	var token bearer.Token
	token.SetEACLTable(eacl.NewTableForContainer(cnr, rs))
	token.SetIat(943083305)
	token.SetNbf(1362292619)
	token.SetExp(1922557325)
	token.ForUser(user.ID{53, 197, 253, 158, 80, 99, 216, 15, 163, 84, 226, 85, 39, 60, 184, 69, 174, 239, 83, 110, 196,
		73, 91, 193, 40})
	return token
}

func init() {
	noIssuerBearerECDSASHA512 = getUnsignedNoIssuerBearerToken()
	noIssuerBearerECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 204, 76, 110, 86, 216, 105, 252, 77, 37, 76, 1, 226, 63, 139, 117, 10, 120, 248, 158, 162, 6, 183, 176, 137, 239,
			94, 180, 219, 189, 31, 90, 122, 1, 195, 159, 41, 114, 234, 213, 88, 222, 145, 131, 50, 227, 5, 66, 223, 95, 169, 143,
			214, 204, 21, 225, 250, 181, 185, 191, 30, 57, 85, 174, 37}))
	noIssuerBearerECDSARFC6979 = getUnsignedNoIssuerBearerToken()
	noIssuerBearerECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{51, 140, 113, 50, 171, 210, 111, 39, 165, 27, 16, 197, 71, 125, 117, 244, 110, 24, 38, 210, 45, 162, 231, 89, 67, 194,
			60, 5, 120, 166, 40, 2, 2, 127, 135, 127, 52, 92, 185, 190, 133, 96, 191, 102, 173, 170, 248, 31, 250, 220, 69, 94,
			188, 223, 15, 63, 207, 86, 244, 45, 175, 48, 203, 184}))
	noIssuerBearerECDSAWalletConnect = getUnsignedNoIssuerBearerToken()
	noIssuerBearerECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{123, 167, 61, 198, 10, 150, 163, 146, 229, 232, 111, 52, 205, 136, 25, 159, 186, 198, 186, 174, 54, 137, 219, 228,
			150, 10, 98, 118, 56, 109, 215, 206, 170, 13, 160, 32, 231, 223, 201, 103, 150, 153, 126, 135, 124, 37, 251, 150, 213,
			166, 93, 163, 79, 214, 57, 50, 165, 188, 210, 154, 54, 207, 50, 107, 85, 161, 160, 44, 251, 105, 236, 34, 130, 208, 153,
			113, 8, 227, 37, 171}))

	bearerECDSASHA512 = getUnsignedBearerToken()
	bearerECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 24, 46, 114, 115, 87, 30, 161, 12, 157, 233, 102, 164, 152, 92, 252, 86, 217, 60, 103, 248, 192, 199, 197, 191, 28,
			203, 118, 79, 196, 159, 38, 197, 177, 204, 10, 234, 35, 233, 233, 218, 53, 99, 68, 11, 112, 255, 66, 15, 245, 184, 45,
			78, 6, 97, 7, 45, 205, 7, 180, 192, 160, 167, 122, 80}))
	bearerECDSARFC6979 = getUnsignedBearerToken()
	bearerECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{88, 202, 56, 197, 167, 0, 36, 144, 69, 8, 212, 61, 92, 95, 253, 41, 29, 235, 215, 207, 184, 191, 120, 217, 186, 51,
			242, 206, 1, 156, 112, 106, 123, 157, 161, 118, 157, 205, 12, 211, 82, 170, 145, 113, 199, 3, 165, 120, 136, 98, 190, 219,
			68, 21, 31, 50, 241, 117, 201, 180, 48, 158, 221, 16}))
	bearerECDSAWalletConnect = getUnsignedBearerToken()
	bearerECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{57, 245, 245, 144, 82, 146, 66, 67, 245, 218, 107, 248, 31, 102, 61, 191, 222, 19, 209, 39, 241, 108, 17, 133, 114,
			69, 110, 246, 134, 80, 93, 124, 31, 128, 34, 19, 225, 139, 194, 206, 251, 5, 213, 225, 183, 57, 213, 137, 42, 208,
			183, 63, 182, 215, 85, 186, 134, 184, 110, 126, 158, 1, 229, 168, 17, 123, 106, 49, 155, 41, 80, 189, 245, 157, 118, 231,
			41, 180, 98, 13}))

	otherPub := []byte{2, 18, 34, 97, 195, 72, 248, 248, 66, 115, 245, 186, 17, 9, 23, 36, 190, 200, 252, 149, 126, 120, 53, 48, 171,
		186, 224, 156, 27, 201, 159, 104, 92}
	wrongIssuerBearerECDSASHA512 = bearerECDSASHA512
	wrongIssuerBearerECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, otherPub,
		[]byte{4, 149, 112, 134, 163, 132, 177, 69, 213, 220, 15, 219, 60, 3, 231, 105, 153, 250, 170, 21, 97, 133, 146, 187, 18, 88,
			224, 111, 204, 228, 173, 118, 204, 137, 85, 37, 229, 252, 235, 207, 114, 164, 68, 190, 172, 78, 48, 133, 62, 162,
			233, 118, 84, 151, 112, 97, 87, 147, 47, 204, 156, 2, 63, 176, 226}))
	wrongIssuerBearerECDSARFC6979 = bearerECDSARFC6979
	wrongIssuerBearerECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, otherPub,
		[]byte{200, 241, 96, 120, 61, 226, 106, 32, 3, 154, 235, 208, 38, 128, 128, 183, 24, 109, 129, 121, 59, 217, 130, 72, 33,
			241, 159, 158, 57, 130, 241, 222, 190, 158, 242, 150, 95, 53, 252, 91, 169, 201, 31, 168, 177, 119, 102, 42, 53, 246, 14,
			8, 189, 202, 89, 149, 66, 210, 19, 163, 158, 73, 251, 229}))
	wrongIssuerBearerECDSAWalletConnect = bearerECDSAWalletConnect
	wrongIssuerBearerECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, otherPub,
		[]byte{151, 226, 103, 118, 100, 52, 132, 123, 140, 69, 58, 110, 77, 17, 120, 108, 70, 175, 49, 189, 117, 176, 167, 215, 197, 8, 152,
			62, 8, 129, 191, 86, 49, 234, 195, 78, 83, 71, 233, 51, 237, 174, 241, 143, 121, 250, 200, 241, 162, 83, 184, 19, 190, 194,
			151, 99, 93, 200, 26, 85, 17, 41, 179, 172, 175, 95, 212, 127, 133, 21, 110, 216, 80, 66, 19, 206, 161, 39, 160, 252}))
}

func getUnsignedObjectSessionToken() session.Object {
	token := getUnsignedNoIssuerObjectSessionToken()
	token.SetIssuer(issuer)
	return token
}

func getUnsignedNoIssuerObjectSessionToken() session.Object {
	var token session.Object
	token.SetID(uuid.UUID{42, 205, 175, 109, 117, 103, 72, 183, 136, 206, 246, 166, 95, 163, 26, 40})
	token.SetIat(943083305)
	token.SetNbf(1362292619)
	token.SetExp(1922557325)
	token.SetAuthKey((*neofsecdsa.PublicKey)(&ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X: new(big.Int).SetBytes([]byte{220, 224, 31, 226, 169, 69, 255, 105, 70, 179, 115, 1, 105, 169, 19, 197, 6, 105, 51, 122,
			138, 225, 171, 48, 158, 92, 142, 63, 9, 26, 146, 128}),
		Y: new(big.Int).SetBytes([]byte{154, 136, 236, 210, 242, 177, 50, 23, 63, 115, 1, 203, 226, 209, 251, 207, 173, 33, 193,
			71, 37, 47, 58, 133, 199, 71, 80, 46, 210, 6, 172, 249}),
	}))
	token.BindContainer(cid.ID{61, 208, 16, 128, 106, 78, 90, 196, 156, 65, 180, 142, 62, 137, 245, 242, 69, 250, 212, 176, 35, 114,
		239, 114, 53, 231, 19, 14, 46, 67, 163, 155})
	token.LimitByObjects(
		oid.ID{186, 250, 155, 51, 135, 247, 169, 197, 204, 217, 12, 121, 139, 150, 213, 156, 16, 26, 42, 140, 205, 45, 60, 34, 14, 135,
			136, 156, 253, 73, 190, 206},
		oid.ID{22, 112, 5, 244, 76, 7, 244, 67, 149, 47, 173, 143, 106, 119, 196, 71, 101, 107, 191, 122, 226, 93, 70, 200, 186, 251,
			170, 171, 248, 20, 26, 18},
	)
	return token
}

func init() {
	noIssuerObjectSessionECDSASHA512 = getUnsignedNoIssuerObjectSessionToken()
	noIssuerObjectSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 243, 14, 177, 173, 198, 253, 187, 178, 38, 39, 215, 229, 237, 251, 226, 56, 108, 183, 54, 78, 187, 113, 138, 79, 125,
			41, 42, 44, 137, 24, 208, 93, 71, 131, 132, 196, 69, 161, 118, 143, 221, 174, 93, 220, 134, 190, 26, 163, 209, 246, 17,
			83, 134, 1, 127, 137, 8, 186, 61, 55, 245, 213, 91, 86}))
	noIssuerObjectSessionECDSARFC6979 = getUnsignedNoIssuerObjectSessionToken()
	noIssuerObjectSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{113, 100, 121, 226, 12, 101, 211, 239, 192, 199, 32, 57, 206, 117, 214, 145, 52, 116, 234, 150, 0, 156, 40, 210, 253,
			125, 193, 43, 193, 111, 165, 7, 130, 206, 151, 45, 90, 137, 83, 8, 255, 64, 185, 52, 203, 221, 218, 104, 243, 64, 157, 11,
			32, 5, 77, 90, 122, 4, 136, 5, 136, 26, 240, 20}))
	noIssuerObjectSessionECDSAWalletConnect = getUnsignedNoIssuerObjectSessionToken()
	noIssuerObjectSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{65, 219, 42, 75, 78, 144, 98, 151, 144, 52, 221, 178, 67, 179, 51, 105, 30, 52, 107, 81, 107, 16, 205, 214, 94, 47, 220,
			157, 246, 150, 105, 96, 105, 255, 143, 109, 61, 56, 70, 168, 239, 250, 63, 114, 95, 0, 156, 216, 111, 164, 61, 75, 14, 230,
			150, 85, 33, 222, 203, 71, 61, 235, 242, 246, 68, 98, 248, 14, 178, 198, 11, 118, 10, 217, 64, 95, 80, 89, 143, 183}))

	objectSessionECDSASHA512 = getUnsignedObjectSessionToken()
	objectSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 28, 202, 157, 26, 79, 157, 138, 57, 192, 241, 26, 117, 143, 238, 99, 237, 77, 253, 163, 29, 221, 221, 120, 79, 1,
			109, 144, 38, 139, 47, 229, 205, 142, 7, 247, 232, 105, 251, 138, 23, 70, 26, 11, 233, 173, 47, 147, 4, 39, 22, 216,
			128, 225, 172, 182, 41, 120, 4, 141, 244, 127, 110, 131, 204}))
	objectSessionECDSARFC6979 = getUnsignedObjectSessionToken()
	objectSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{87, 5, 15, 37, 110, 117, 51, 217, 92, 117, 65, 121, 125, 215, 187, 223, 133, 29, 137, 208, 244, 224, 37, 111, 66, 198,
			189, 37, 141, 240, 9, 180, 43, 162, 166, 192, 2, 227, 156, 176, 177, 120, 62, 241, 54, 58, 91, 148, 162, 36, 213, 174, 214,
			150, 14, 227, 153, 245, 37, 32, 50, 34, 237, 213}))
	objectSessionECDSAWalletConnect = getUnsignedObjectSessionToken()
	objectSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{207, 27, 192, 58, 184, 90, 215, 225, 19, 236, 90, 142, 217, 197, 154, 148, 131, 11, 24, 162, 2, 124, 108, 100, 180, 193,
			149, 129, 150, 123, 21, 212, 250, 62, 69, 218, 114, 226, 255, 231, 162, 214, 244, 105, 102, 63, 111, 236, 158, 245, 193,
			220, 142, 244, 164, 16, 190, 42, 165, 71, 174, 247, 246, 23, 122, 94, 24, 203, 121, 174, 47, 174, 62, 65, 101, 140, 173,
			123, 43, 238}))

	otherPub := []byte{2, 139, 48, 108, 130, 11, 53, 126, 210, 151, 229, 248, 130, 199, 34, 40, 3, 183, 115, 197, 206, 239, 202, 248,
		222, 155, 186, 221, 11, 68, 161, 177, 253}
	wrongIssuerObjectSessionECDSASHA512 = objectSessionECDSASHA512
	wrongIssuerObjectSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, otherPub,
		[]byte{4, 241, 20, 145, 98, 203, 90, 123, 249, 49, 243, 210, 197, 233, 139, 97, 55, 241, 47, 82, 229, 200, 190, 66, 243,
			63, 32, 20, 217, 25, 31, 75, 118, 195, 154, 45, 127, 203, 217, 82, 176, 187, 78, 195, 73, 166, 12, 193, 20, 9, 97, 142,
			215, 56, 82, 34, 100, 92, 207, 39, 219, 67, 109, 176, 20}))
	wrongIssuerObjectSessionECDSARFC6979 = objectSessionECDSARFC6979
	wrongIssuerObjectSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, otherPub,
		[]byte{64, 160, 40, 82, 234, 43, 99, 28, 140, 150, 103, 134, 179, 4, 211, 195, 146, 10, 194, 99, 22, 43, 129, 114, 215, 85, 195,
			171, 88, 160, 172, 152, 31, 210, 254, 120, 1, 104, 65, 137, 183, 147, 66, 194, 84, 28, 3, 147, 86, 196, 210, 129, 64, 254,
			104, 81, 193, 135, 223, 180, 37, 236, 15, 13}))
	wrongIssuerObjectSessionECDSAWalletConnect = objectSessionECDSAWalletConnect
	wrongIssuerObjectSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, otherPub,
		[]byte{222, 98, 253, 242, 184, 230, 122, 121, 169, 18, 140, 181, 208, 22, 154, 9, 216, 214, 197, 150, 204, 51, 189, 246, 187,
			28, 87, 118, 104, 169, 204, 130, 217, 36, 205, 25, 24, 137, 25, 24, 174, 208, 217, 231, 59, 123, 211, 209, 241, 36, 58,
			224, 97, 155, 2, 112, 54, 99, 18, 137, 120, 124, 18, 164, 78, 123, 191, 255, 127, 72, 184, 78, 58, 248, 214, 194, 172, 137,
			254, 97}))
}

func getUnsignedContainerSessionToken() session.Container {
	token := getUnsignedNoIssuerContainerSessionToken()
	token.SetIssuer(issuer)
	return token
}

func getUnsignedNoIssuerContainerSessionToken() session.Container {
	var token session.Container
	token.SetID(uuid.UUID{42, 205, 175, 109, 117, 103, 72, 183, 136, 206, 246, 166, 95, 163, 26, 40})
	token.SetIat(943083305)
	token.SetNbf(1362292619)
	token.SetExp(1922557325)
	token.SetAuthKey((*neofsecdsa.PublicKey)(&ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X: new(big.Int).SetBytes([]byte{220, 224, 31, 226, 169, 69, 255, 105, 70, 179, 115, 1, 105, 169, 19, 197, 6, 105, 51, 122,
			138, 225, 171, 48, 158, 92, 142, 63, 9, 26, 146, 128}),
		Y: new(big.Int).SetBytes([]byte{154, 136, 236, 210, 242, 177, 50, 23, 63, 115, 1, 203, 226, 209, 251, 207, 173, 33, 193,
			71, 37, 47, 58, 133, 199, 71, 80, 46, 210, 6, 172, 249}),
	}))
	token.ApplyOnlyTo(cid.ID{61, 208, 16, 128, 106, 78, 90, 196, 156, 65, 180, 142, 62, 137, 245, 242, 69, 250, 212, 176, 35, 114,
		239, 114, 53, 231, 19, 14, 46, 67, 163, 155})
	token.ForVerb(1762573065)
	return token
}

func init() {
	noIssuerContainerSessionECDSASHA512 = getUnsignedNoIssuerContainerSessionToken()
	noIssuerContainerSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 204, 80, 21, 241, 156, 27, 25, 244, 157, 202, 127, 98, 86, 13, 2, 74, 72, 76, 108, 189, 202, 170, 221, 119, 20,
			22, 149, 19, 90, 87, 50, 117, 147, 21, 162, 18, 226, 5, 106, 160, 26, 119, 209, 16, 102, 196, 33, 144, 113, 170, 150, 4, 2,
			22, 187, 63, 215, 18, 186, 240, 128, 163, 244, 121}))
	noIssuerContainerSessionECDSARFC6979 = getUnsignedNoIssuerContainerSessionToken()
	noIssuerContainerSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{68, 49, 22, 94, 69, 250, 135, 121, 37, 107, 31, 199, 212, 57, 175, 29, 87, 196, 60, 116, 114, 251, 167, 1, 211, 249, 38,
			59, 229, 8, 48, 9, 203, 255, 230, 86, 202, 23, 44, 86, 195, 20, 186, 188, 39, 191, 178, 235, 153, 107, 72, 16, 47, 96,
			229, 107, 113, 158, 215, 236, 217, 246, 237, 238}))
	noIssuerContainerSessionECDSAWalletConnect = getUnsignedNoIssuerContainerSessionToken()
	noIssuerContainerSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{100, 66, 51, 67, 244, 236, 20, 108, 176, 57, 207, 145, 206, 21, 247, 126, 165, 25, 151, 245, 173, 140, 173, 194, 169, 21,
			185, 100, 110, 151, 189, 123, 237, 167, 190, 37, 12, 126, 48, 53, 111, 232, 1, 87, 143, 31, 206, 203, 21, 74, 162, 140, 124,
			28, 80, 36, 149, 14, 74, 178, 125, 51, 211, 189, 227, 20, 180, 194, 165, 2, 124, 202, 207, 253, 61, 159, 6, 62, 9, 71}))

	containerSessionECDSASHA512 = getUnsignedContainerSessionToken()
	containerSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA,
		[]byte{4, 246, 8, 104, 140, 174, 164, 156, 136, 51, 29, 220, 22, 140, 66, 194, 117, 0, 136, 161, 36, 149, 15, 198, 223, 67, 245,
			105, 188, 250, 237, 233, 128, 143, 192, 88, 86, 251, 221, 63, 215, 35, 61, 192, 162, 181, 17, 221, 232, 239, 108, 36,
			216, 31, 36, 12, 122, 47, 139, 205, 164, 148, 121, 244, 214}))
	containerSessionECDSARFC6979 = getUnsignedContainerSessionToken()
	containerSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA,
		[]byte{36, 211, 229, 6, 32, 80, 212, 215, 189, 64, 218, 18, 16, 131, 170, 38, 177, 51, 203, 110, 71, 76, 228, 60, 105, 189, 142,
			25, 187, 163, 82, 8, 122, 129, 251, 253, 159, 29, 248, 177, 121, 215, 58, 231, 48, 217, 226, 47, 136, 20, 36, 252, 211,
			58, 40, 76, 178, 105, 190, 173, 35, 153, 9, 144}))
	containerSessionECDSAWalletConnect = getUnsignedContainerSessionToken()
	containerSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA,
		[]byte{98, 180, 29, 63, 253, 79, 106, 117, 129, 135, 175, 73, 114, 210, 60, 144, 188, 163, 68, 18, 78, 164, 219, 69, 9, 198, 123, 31,
			251, 31, 21, 161, 30, 113, 97, 64, 194, 113, 213, 194, 54, 134, 39, 35, 248, 30, 182, 45, 218, 107, 187, 23, 13, 202, 248,
			106, 212, 116, 159, 233, 156, 186, 225, 246, 50, 229, 124, 186, 177, 197, 150, 242, 192, 108, 99, 220, 104, 50, 4, 54}))

	otherPub := []byte{2, 103, 251, 163, 16, 171, 89, 118, 221, 200, 57, 144, 52, 40, 249, 105, 207, 66, 8, 224, 119, 218, 254, 246,
		247, 89, 233, 115, 72, 80, 157, 160, 98}
	wrongIssuerContainerSessionECDSASHA512 = containerSessionECDSASHA512
	wrongIssuerContainerSessionECDSASHA512.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, otherPub,
		[]byte{4, 90, 212, 194, 79, 200, 47, 78, 38, 46, 127, 116, 222, 206, 180, 142, 251, 206, 56, 172, 74, 184, 198, 58, 84, 75,
			78, 81, 139, 105, 11, 231, 127, 225, 42, 187, 166, 206, 222, 247, 186, 18, 76, 34, 201, 187, 55, 247, 221, 81, 72, 127,
			75, 84, 92, 226, 32, 143, 142, 229, 97, 31, 136, 32, 53}))
	wrongIssuerContainerSessionECDSARFC6979 = containerSessionECDSARFC6979
	wrongIssuerContainerSessionECDSARFC6979.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, otherPub,
		[]byte{99, 72, 158, 32, 130, 242, 156, 103, 207, 162, 158, 147, 17, 98, 98, 228, 195, 253, 42, 92, 17, 113, 76, 22, 57, 215,
			127, 160, 170, 87, 64, 136, 111, 11, 165, 141, 173, 29, 53, 183, 8, 187, 107, 218, 116, 215, 167, 125, 109, 202, 147, 63, 42,
			208, 211, 62, 182, 176, 135, 6, 48, 208, 23, 52}))
	wrongIssuerContainerSessionECDSAWalletConnect = containerSessionECDSAWalletConnect
	wrongIssuerContainerSessionECDSAWalletConnect.AttachSignature(neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, otherPub,
		[]byte{176, 85, 169, 186, 140, 245, 21, 14, 235, 93, 174, 110, 99, 50, 47, 150, 137, 95, 152, 16, 204, 175, 59, 239, 119, 41, 187,
			156, 216, 138, 64, 35, 221, 128, 125, 213, 130, 28, 78, 162, 67, 221, 105, 7, 238, 131, 198, 242, 183, 149, 138, 185, 105,
			112, 237, 16, 253, 70, 157, 244, 197, 151, 38, 195, 120, 190, 131, 244, 138, 201, 197, 173, 107, 3, 205, 57, 240, 211, 209, 201}))
}
