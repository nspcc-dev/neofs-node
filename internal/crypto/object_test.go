package crypto_test

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateObject(t *testing.T) {
	t.Run("without signature", func(t *testing.T) {
		obj := getUnsignedObject()
		require.EqualError(t, icrypto.AuthenticateObject(obj), "missing signature")
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		obj := objectECDSASHA512
		sig := *obj.Signature()
		sig.SetScheme(3)
		obj.SetSignature(&sig)
		require.EqualError(t, icrypto.AuthenticateObject(obj), "unsupported scheme 3")
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
				obj := objectECDSASHA512
				sig := *obj.Signature()
				sig.SetPublicKeyBytes(tc.changePub(sig.PublicKeyBytes()))
				obj.SetSignature(&sig)
				err := icrypto.AuthenticateObject(obj)
				require.EqualError(t, err, "scheme ECDSA_SHA512: decode public key: "+tc.err)
			})
		}
	})
	t.Run("signature mismatch", func(t *testing.T) {
		for _, tc := range []struct {
			scheme neofscrypto.Scheme
			obj    object.Object
		}{
			{neofscrypto.ECDSA_SHA512, objectECDSASHA512},
			{neofscrypto.ECDSA_DETERMINISTIC_SHA256, objectECDSARFC6979},
			{neofscrypto.ECDSA_WALLETCONNECT, objectECDSAWalletConnect},
		} {
			t.Run(tc.scheme.String(), func(t *testing.T) {
				sig := *tc.obj.Signature()
				validSig := sig.Value()
				for i := range validSig {
					cp := slices.Clone(validSig)
					cp[i]++
					sig.SetValue(cp)
					tc.obj.SetSignature(&sig)
					err := icrypto.AuthenticateObject(tc.obj)
					require.EqualError(t, err, fmt.Sprintf("scheme %s: signature mismatch", tc.scheme))
				}
			})
		}
	})
	t.Run("invalid session", func(t *testing.T) {
		t.Run("no issuer", func(t *testing.T) {
			for _, tc := range []struct {
				scheme neofscrypto.Scheme
				object object.Object
			}{
				{scheme: neofscrypto.ECDSA_SHA512, object: objectWithNoIssuerSessionECDSASHA512},
				{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: objectWithNoIssuerSessionECDSARFC6979},
				{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: objectWithNoIssuerSessionECDSAWalletConnect},
			} {
				t.Run(tc.scheme.String(), func(t *testing.T) {
					require.EqualError(t, icrypto.AuthenticateObject(tc.object), "session token: missing issuer")
				})
			}
		})
		t.Run("wrong issuer", func(t *testing.T) {
			for _, tc := range []struct {
				scheme neofscrypto.Scheme
				object object.Object
			}{
				{scheme: neofscrypto.ECDSA_SHA512, object: objectWithWrongIssuerSessionECDSASHA512},
				{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: objectWithWrongIssuerSessionECDSARFC6979},
				{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: objectWithWrongIssuerSessionECDSAWalletConnect},
			} {
				t.Run(tc.scheme.String(), func(t *testing.T) {
					require.EqualError(t, icrypto.AuthenticateObject(tc.object), "session token: issuer mismatches signature")
				})
			}
		})
		t.Run("wrong subject", func(t *testing.T) {
			for _, tc := range []struct {
				scheme neofscrypto.Scheme
				object object.Object
			}{
				{scheme: neofscrypto.ECDSA_SHA512, object: objectWithWrongSessionSubjectECDSASHA512},
				{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: objectWithWrongSessionSubjectECDSARFC6979},
				{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: objectWithWrongSessionSubjectECDSAWalletConnect},
			} {
				t.Run(tc.scheme.String(), func(t *testing.T) {
					require.EqualError(t, icrypto.AuthenticateObject(tc.object), "session token is not for object's signer")
				})
			}
		})
		t.Run("issuer differs owner", func(t *testing.T) {
			for _, tc := range []struct {
				scheme neofscrypto.Scheme
				object object.Object
			}{
				{scheme: neofscrypto.ECDSA_SHA512, object: objectWithWrongOwnerSessionECDSASHA512},
				{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: objectWithWrongOwnerSessionECDSARFC6979},
				{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: objectWithWrongOwnerSessionECDSAWalletConnect},
			} {
				t.Run(tc.scheme.String(), func(t *testing.T) {
					require.EqualError(t, icrypto.AuthenticateObject(tc.object), "different object owner and session issuer")
				})
			}
		})
	})
	for _, tc := range []struct {
		name   string
		object object.Object
	}{
		{name: neofscrypto.ECDSA_SHA512.String(), object: objectECDSASHA512},
		{name: neofscrypto.ECDSA_DETERMINISTIC_SHA256.String(), object: objectECDSARFC6979},
		{name: neofscrypto.ECDSA_WALLETCONNECT.String(), object: objectECDSAWalletConnect},
		{name: neofscrypto.ECDSA_SHA512.String() + " with session", object: objectWithSessionECDSASHA512},
		{name: neofscrypto.ECDSA_DETERMINISTIC_SHA256.String() + " with session", object: objectWithSessionECDSARFC6979},
		{name: neofscrypto.ECDSA_WALLETCONNECT.String() + " with session", object: objectWithSessionECDSAWalletConnect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, icrypto.AuthenticateObject(tc.object))
		})
	}
}

// set in init.
var (
	objectECDSASHA512        object.Object
	objectECDSARFC6979       object.Object
	objectECDSAWalletConnect object.Object

	objectWithSessionECDSASHA512        object.Object
	objectWithSessionECDSARFC6979       object.Object
	objectWithSessionECDSAWalletConnect object.Object

	objectWithNoIssuerSessionECDSASHA512        object.Object
	objectWithNoIssuerSessionECDSARFC6979       object.Object
	objectWithNoIssuerSessionECDSAWalletConnect object.Object

	objectWithWrongIssuerSessionECDSASHA512        object.Object
	objectWithWrongIssuerSessionECDSARFC6979       object.Object
	objectWithWrongIssuerSessionECDSAWalletConnect object.Object

	objectWithWrongSessionSubjectECDSASHA512        object.Object
	objectWithWrongSessionSubjectECDSARFC6979       object.Object
	objectWithWrongSessionSubjectECDSAWalletConnect object.Object

	objectWithWrongOwnerSessionECDSASHA512        object.Object
	objectWithWrongOwnerSessionECDSARFC6979       object.Object
	objectWithWrongOwnerSessionECDSAWalletConnect object.Object
)

func getUnsignedObject() object.Object {
	const creationEpoch = 1362292619
	const typ = 43308543
	ver := version.New(123, 456)
	cnr := cid.ID{61, 208, 16, 128, 106, 78, 90, 196, 156, 65, 180, 142, 62, 137, 245, 242, 69, 250, 212, 176, 35, 114, 239, 114, 53,
		231, 19, 14, 46, 67, 163, 155}
	childPayload := []byte("Hello,")
	payload := slices.Concat(childPayload, []byte("world!"))

	var par object.Object
	par.SetPayload(payload)
	par.SetPayloadSize(uint64(len(payload)))
	par.SetVersion(&ver)
	par.SetContainerID(cnr)
	par.SetOwner(mainAcc)
	par.SetCreationEpoch(creationEpoch)
	par.SetType(typ)
	par.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(payload)))
	par.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(payload)))
	par.SetAttributes(*object.NewAttribute("690530953", "39483258"), *object.NewAttribute("7208331", "31080424839"))
	par.SetID(oid.ID{156, 209, 245, 87, 177, 145, 183, 8, 181, 92, 171, 193, 58, 125, 77, 11, 28, 161, 217, 151, 17, 212, 232, 88, 6,
		180, 184, 86, 250, 85, 25, 180})

	var child object.Object
	child.SetPayload(childPayload)
	child.SetPayloadSize(uint64(len(childPayload)))
	child.SetVersion(&ver)
	child.SetContainerID(cnr)
	child.SetOwner(mainAcc)
	child.SetCreationEpoch(creationEpoch)
	child.SetType(typ)
	child.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(childPayload)))
	child.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(childPayload)))
	child.SetAttributes(*object.NewAttribute("31429585", "689450942"), *object.NewAttribute("129849325", "859384298"))
	child.SetParent(&par)
	child.SetID(oid.ID{44, 189, 3, 215, 147, 16, 210, 126, 212, 84, 39, 229, 126, 136, 236, 141, 39, 54, 124, 41, 200, 92, 84, 6,
		172, 47, 157, 83, 58, 189, 86, 66})

	return child
}

func init() {
	objectECDSASHA512 = getUnsignedObject()
	objectECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, mainAccECDSAPub, []byte{
		4, 71, 28, 84, 123, 105, 234, 69, 202, 194, 209, 164, 65, 119, 34, 208, 156, 231, 86, 166, 165, 201, 239, 174, 99, 141, 97,
		232, 189, 110, 45, 255, 38, 138, 227, 25, 207, 201, 102, 147, 23, 236, 184, 93, 32, 234, 140, 132, 86, 113, 120, 174,
		94, 169, 82, 16, 201, 222, 123, 142, 108, 77, 240, 133, 169,
	})
	objectECDSASHA512.SetSignature(&objectECDSASHA512Sig)

	objectECDSARFC6979 = getUnsignedObject()
	objectECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, mainAccECDSAPub, []byte{
		210, 228, 126, 228, 138, 136, 106, 9, 183, 216, 161, 144, 88, 132, 127, 202, 140, 104, 55, 86, 37, 184, 216, 198, 78, 190,
		80, 103, 159, 86, 135, 171, 73, 129, 182, 156, 12, 48, 32, 203, 140, 250, 252, 204, 68, 90, 209, 101, 154, 181, 200, 250,
		187, 41, 15, 91, 104, 59, 33, 26, 253, 146, 175, 149,
	})
	objectECDSARFC6979.SetSignature(&objectECDSARFC6979Sig)

	objectECDSAWalletConnect = getUnsignedObject()
	objectECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, mainAccECDSAPub, []byte{
		33, 149, 185, 65, 225, 27, 75, 150, 71, 0, 209, 27, 7, 105, 81, 133, 40, 173, 188, 105, 150, 49, 186, 168, 191, 205, 131, 143,
		125, 97, 186, 213, 71, 9, 245, 58, 208, 5, 31, 83, 101, 180, 187, 136, 50, 162, 93, 8, 128, 70, 98, 242, 168, 224, 182, 114,
		207, 208, 124, 88, 167, 105, 38, 30, 58, 23, 117, 77, 156, 112, 171, 163, 80, 38, 69, 50, 155, 253, 1, 236,
	})
	objectECDSAWalletConnect.SetSignature(&objectECDSAWalletConnectSig)

	objectWithSessionECDSASHA512 = getUnsignedObject()
	objectWithSessionECDSASHA512.SetSessionToken(&objectSessionECDSASHA512)
	objectWithSessionECDSASHA512.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100, 16, 41,
		157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithSessionECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, sessionSubjECDSAPub, []byte{
		4, 29, 139, 241, 145, 198, 129, 190, 243, 18, 168, 29, 140, 170, 93, 30, 145, 89, 76, 144, 71, 134, 138, 26, 239, 238, 231,
		190, 178, 175, 117, 75, 137, 129, 14, 80, 76, 82, 229, 0, 205, 109, 87, 109, 28, 236, 59, 86, 22, 195, 52, 209, 95, 150, 46,
		106, 251, 60, 5, 170, 246, 189, 228, 184, 215,
	})
	objectWithSessionECDSASHA512.SetSignature(&objectWithSessionECDSASHA512Sig)

	objectWithSessionECDSARFC6979 = getUnsignedObject()
	objectWithSessionECDSARFC6979.SetSessionToken(&objectSessionECDSARFC6979)
	objectWithSessionECDSARFC6979.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100, 16, 41,
		157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithSessionECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, sessionSubjECDSAPub, []byte{
		247, 75, 217, 203, 45, 140, 247, 36, 7, 94, 53, 187, 175, 101, 110, 99, 164, 249, 188, 167, 171, 247, 234, 246, 160, 8, 42,
		124, 136, 28, 220, 8, 215, 207, 85, 54, 162, 227, 151, 255, 223, 33, 85, 234, 36, 224, 177, 87, 90, 153, 195, 52, 10,
		185, 40, 238, 242, 32, 41, 213, 127, 185, 193, 178,
	})
	objectWithSessionECDSARFC6979.SetSignature(&objectWithSessionECDSARFC6979Sig)

	objectWithSessionECDSAWalletConnect = getUnsignedObject()
	objectWithSessionECDSAWalletConnect.SetSessionToken(&objectSessionECDSAWalletConnect)
	objectWithSessionECDSAWalletConnect.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100,
		16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithSessionECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, sessionSubjECDSAPub, []byte{
		90, 29, 243, 125, 89, 142, 114, 232, 86, 150, 236, 28, 31, 107, 105, 35, 82, 12, 197, 142, 213, 149, 138, 140, 23, 130,
		126, 45, 74, 211, 117, 10, 202, 76, 190, 248, 114, 138, 41, 115, 8, 226, 215, 86, 230, 238, 27, 198, 202, 139, 19, 216,
		238, 103, 15, 216, 195, 24, 178, 206, 128, 200, 110, 231, 149, 226, 122, 50, 24, 7, 157, 213, 10, 245, 79, 234, 209,
		214, 237, 62,
	})
	objectWithSessionECDSAWalletConnect.SetSignature(&objectWithSessionECDSAWalletConnectSig)

	objectWithNoIssuerSessionECDSASHA512 = getUnsignedObject()
	objectWithNoIssuerSessionECDSASHA512.SetSessionToken(&noIssuerObjectSessionECDSASHA512)
	objectWithNoIssuerSessionECDSASHA512.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100, 16, 41,
		157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithNoIssuerSessionECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, sessionSubjECDSAPub, []byte{
		4, 29, 139, 241, 145, 198, 129, 190, 243, 18, 168, 29, 140, 170, 93, 30, 145, 89, 76, 144, 71, 134, 138, 26, 239, 238, 231,
		190, 178, 175, 117, 75, 137, 129, 14, 80, 76, 82, 229, 0, 205, 109, 87, 109, 28, 236, 59, 86, 22, 195, 52, 209, 95, 150, 46,
		106, 251, 60, 5, 170, 246, 189, 228, 184, 215,
	})
	objectWithNoIssuerSessionECDSASHA512.SetSignature(&objectWithNoIssuerSessionECDSASHA512Sig)

	objectWithNoIssuerSessionECDSARFC6979 = getUnsignedObject()
	objectWithNoIssuerSessionECDSARFC6979.SetSessionToken(&noIssuerObjectSessionECDSARFC6979)
	objectWithNoIssuerSessionECDSARFC6979.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100,
		16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithNoIssuerSessionECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, sessionSubjECDSAPub, []byte{
		247, 75, 217, 203, 45, 140, 247, 36, 7, 94, 53, 187, 175, 101, 110, 99, 164, 249, 188, 167, 171, 247, 234, 246, 160, 8, 42,
		124, 136, 28, 220, 8, 215, 207, 85, 54, 162, 227, 151, 255, 223, 33, 85, 234, 36, 224, 177, 87, 90, 153, 195, 52, 10,
		185, 40, 238, 242, 32, 41, 213, 127, 185, 193, 178,
	})
	objectWithNoIssuerSessionECDSARFC6979.SetSignature(&objectWithNoIssuerSessionECDSARFC6979Sig)

	objectWithNoIssuerSessionECDSAWalletConnect = getUnsignedObject()
	objectWithNoIssuerSessionECDSAWalletConnect.SetSessionToken(&noIssuerObjectSessionECDSAWalletConnect)
	objectWithNoIssuerSessionECDSAWalletConnect.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158,
		58, 100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithNoIssuerSessionECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, sessionSubjECDSAPub, []byte{
		225, 36, 6, 29, 15, 61, 186, 110, 86, 17, 184, 74, 143, 128, 209, 22, 234, 192, 50, 143, 145, 220, 6, 56, 117, 56, 250, 99,
		3, 7, 73, 201, 106, 20, 168, 178, 132, 248, 23, 248, 98, 190, 191, 135, 114, 50, 172, 41, 144, 224, 19, 173, 103, 87, 203,
		64, 61, 76, 46, 164, 75, 253, 75, 166, 33, 81, 253, 26, 209, 130, 137, 106, 80, 164, 151, 47, 19, 62, 1, 114,
	})
	objectWithNoIssuerSessionECDSAWalletConnect.SetSignature(&objectWithNoIssuerSessionECDSAWalletConnectSig)

	objectWithWrongIssuerSessionECDSASHA512 = getUnsignedObject()
	objectWithWrongIssuerSessionECDSASHA512.SetSessionToken(&wrongIssuerObjectSessionECDSASHA512)
	objectWithWrongIssuerSessionECDSASHA512.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongIssuerSessionECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, sessionSubjECDSAPub, []byte{
		4, 1, 186, 162, 242, 131, 201, 67, 155, 24, 44, 69, 0, 237, 17, 115, 123, 98, 226, 103, 16, 34, 98, 8, 62, 173, 16, 178, 197,
		57, 85, 169, 44, 149, 63, 84, 158, 41, 135, 67, 55, 23, 174, 84, 216, 186, 255, 69, 91, 120, 101, 0, 118, 214, 103, 109, 110, 96,
		165, 44, 39, 124, 51, 229, 193,
	})
	objectWithWrongIssuerSessionECDSASHA512.SetSignature(&objectWithWrongIssuerSessionECDSASHA512Sig)

	objectWithWrongIssuerSessionECDSARFC6979 = getUnsignedObject()
	objectWithWrongIssuerSessionECDSARFC6979.SetSessionToken(&wrongIssuerObjectSessionECDSARFC6979)
	objectWithWrongIssuerSessionECDSARFC6979.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongIssuerSessionECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, sessionSubjECDSAPub, []byte{
		247, 75, 217, 203, 45, 140, 247, 36, 7, 94, 53, 187, 175, 101, 110, 99, 164, 249, 188, 167, 171, 247, 234, 246, 160, 8, 42,
		124, 136, 28, 220, 8, 215, 207, 85, 54, 162, 227, 151, 255, 223, 33, 85, 234, 36, 224, 177, 87, 90, 153, 195, 52, 10,
		185, 40, 238, 242, 32, 41, 213, 127, 185, 193, 178,
	})
	objectWithWrongIssuerSessionECDSARFC6979.SetSignature(&objectWithWrongIssuerSessionECDSARFC6979Sig)

	objectWithWrongIssuerSessionECDSAWalletConnect = getUnsignedObject()
	objectWithWrongIssuerSessionECDSAWalletConnect.SetSessionToken(&wrongIssuerObjectSessionECDSAWalletConnect)
	objectWithWrongIssuerSessionECDSAWalletConnect.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongIssuerSessionECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, sessionSubjECDSAPub, []byte{
		70, 198, 113, 32, 157, 136, 40, 78, 193, 203, 151, 83, 33, 176, 4, 66, 237, 152, 131, 33, 84, 196, 209, 196, 64, 52, 86, 82,
		80, 71, 81, 221, 163, 100, 87, 229, 90, 240, 106, 178, 140, 19, 123, 208, 81, 214, 175, 44, 8, 32, 195, 163, 16, 183, 163,
		69, 93, 46, 78, 181, 179, 39, 228, 16, 249, 68, 42, 119, 218, 54, 123, 200, 37, 50, 95, 197, 239, 227, 112, 3,
	})
	objectWithWrongIssuerSessionECDSAWalletConnect.SetSignature(&objectWithWrongIssuerSessionECDSAWalletConnectSig)

	objectWithWrongSessionSubjectECDSASHA512 = getUnsignedObject()
	objectWithWrongSessionSubjectECDSASHA512.SetSessionToken(&objectSessionECDSASHA512)
	objectWithWrongSessionSubjectECDSASHA512.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongSessionSubjectECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, mainAccECDSAPub, []byte{
		4, 28, 214, 237, 248, 64, 122, 58, 69, 19, 237, 46, 125, 226, 128, 253, 52, 225, 21, 69, 103, 65, 193, 199, 15, 40, 218,
		217, 140, 84, 12, 124, 121, 84, 163, 248, 36, 209, 116, 0, 170, 86, 148, 253, 227, 233, 109, 169, 229, 46, 83, 204, 10,
		128, 213, 170, 28, 94, 22, 112, 29, 45, 137, 142, 98,
	})
	objectWithWrongSessionSubjectECDSASHA512.SetSignature(&objectWithWrongSessionSubjectECDSASHA512Sig)

	objectWithWrongSessionSubjectECDSARFC6979 = getUnsignedObject()
	objectWithWrongSessionSubjectECDSARFC6979.SetSessionToken(&objectSessionECDSARFC6979)
	objectWithWrongSessionSubjectECDSARFC6979.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongSessionSubjectECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, mainAccECDSAPub, []byte{
		107, 253, 17, 84, 1, 224, 98, 101, 53, 167, 26, 89, 223, 49, 127, 98, 167, 187, 83, 0, 254, 50, 1, 155, 25, 96, 247, 197,
		44, 65, 81, 71, 86, 248, 232, 234, 140, 157, 75, 111, 205, 226, 86, 236, 119, 67, 174, 242, 107, 239, 51, 161, 190, 46, 47,
		106, 125, 187, 139, 136, 157, 13, 155, 226,
	})
	objectWithWrongSessionSubjectECDSARFC6979.SetSignature(&objectWithWrongSessionSubjectECDSARFC6979Sig)

	objectWithWrongSessionSubjectECDSAWalletConnect = getUnsignedObject()
	objectWithWrongSessionSubjectECDSAWalletConnect.SetSessionToken(&objectSessionECDSAWalletConnect)
	objectWithWrongSessionSubjectECDSAWalletConnect.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58,
		100, 16, 41, 157, 111, 174, 154, 150, 232, 233, 226, 172, 238, 99, 141, 247})
	objectWithWrongSessionSubjectECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, mainAccECDSAPub, []byte{
		246, 65, 39, 4, 139, 110, 250, 237, 83, 130, 2, 10, 235, 128, 46, 130, 35, 253, 143, 0, 24, 110, 96, 73, 178, 228, 79,
		192, 196, 141, 186, 62, 65, 67, 123, 168, 120, 95, 60, 67, 175, 99, 222, 78, 101, 0, 8, 95, 178, 26, 154, 23, 132, 237, 192,
		147, 4, 169, 105, 93, 178, 27, 136, 9, 1, 161, 87, 248, 80, 67, 142, 170, 39, 218, 65, 111, 212, 99, 144, 194,
	})
	objectWithWrongSessionSubjectECDSAWalletConnect.SetSignature(&objectWithWrongSessionSubjectECDSAWalletConnectSig)

	objectWithWrongOwnerSessionECDSASHA512 = getUnsignedObject()
	objectWithWrongOwnerSessionECDSASHA512.SetOwner(otherAcc)
	objectWithWrongOwnerSessionECDSASHA512.SetSessionToken(&objectSessionECDSASHA512)
	objectWithWrongOwnerSessionECDSASHA512.SetID(oid.ID{140, 221, 157, 17, 214, 218, 0, 172, 213, 150, 39, 125, 93, 76, 182,
		104, 70, 152, 37, 200, 250, 118, 179, 78, 237, 106, 202, 248, 241, 47, 218, 50})
	objectWithWrongOwnerSessionECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, sessionSubjECDSAPub, []byte{
		4, 18, 167, 131, 188, 195, 44, 146, 40, 62, 252, 73, 75, 97, 227, 202, 206, 244, 217, 164, 69, 252, 89, 121, 88, 86, 131,
		96, 45, 157, 139, 33, 221, 143, 103, 107, 249, 23, 7, 17, 110, 108, 180, 182, 178, 207, 219, 31, 140, 51, 228, 85, 151, 201,
		45, 197, 30, 208, 145, 156, 253, 1, 215, 63, 5,
	})
	objectWithWrongOwnerSessionECDSASHA512.SetSignature(&objectWithWrongOwnerSessionECDSASHA512Sig)

	objectWithWrongOwnerSessionECDSARFC6979 = getUnsignedObject()
	objectWithWrongOwnerSessionECDSARFC6979.SetOwner(otherAcc)
	objectWithWrongOwnerSessionECDSARFC6979.SetSessionToken(&objectSessionECDSARFC6979)
	objectWithWrongOwnerSessionECDSARFC6979.SetID(oid.ID{140, 221, 157, 17, 214, 218, 0, 172, 213, 150, 39, 125, 93, 76, 182,
		104, 70, 152, 37, 200, 250, 118, 179, 78, 237, 106, 202, 248, 241, 47, 218, 50})
	objectWithWrongOwnerSessionECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, sessionSubjECDSAPub, []byte{
		149, 106, 66, 91, 136, 4, 66, 43, 76, 193, 95, 83, 22, 199, 11, 4, 13, 213, 219, 120, 215, 104, 76, 79, 233, 120, 176, 190,
		119, 154, 49, 168, 96, 112, 51, 4, 120, 246, 57, 57, 84, 37, 106, 239, 80, 132, 180, 184, 153, 252, 56, 187, 166, 31, 59, 67,
		104, 85, 29, 58, 132, 229, 77, 226,
	})
	objectWithWrongOwnerSessionECDSARFC6979.SetSignature(&objectWithWrongOwnerSessionECDSARFC6979Sig)

	objectWithWrongOwnerSessionECDSAWalletConnect = getUnsignedObject()
	objectWithWrongOwnerSessionECDSAWalletConnect.SetOwner(otherAcc)
	objectWithWrongOwnerSessionECDSAWalletConnect.SetSessionToken(&objectSessionECDSAWalletConnect)
	objectWithWrongOwnerSessionECDSAWalletConnect.SetID(oid.ID{140, 221, 157, 17, 214, 218, 0, 172, 213, 150, 39, 125, 93, 76, 182,
		104, 70, 152, 37, 200, 250, 118, 179, 78, 237, 106, 202, 248, 241, 47, 218, 50})
	objectWithWrongOwnerSessionECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, sessionSubjECDSAPub, []byte{
		135, 20, 195, 98, 205, 38, 252, 33, 51, 78, 96, 91, 164, 56, 111, 181, 50, 104, 200, 222, 221, 82, 184, 57, 120, 70, 91,
		236, 3, 166, 233, 78, 178, 4, 177, 157, 49, 225, 118, 9, 82, 130, 202, 240, 131, 252, 185, 127, 199, 8, 116, 164, 106, 173,
		23, 56, 163, 197, 96, 243, 75, 222, 8, 0, 135, 21, 129, 252, 121, 97, 54, 151, 72, 254, 185, 201, 149, 129, 76, 67,
	})
	objectWithWrongOwnerSessionECDSAWalletConnect.SetSignature(&objectWithWrongOwnerSessionECDSAWalletConnectSig)
}
