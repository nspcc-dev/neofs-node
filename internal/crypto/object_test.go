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
	for _, tc := range []struct {
		scheme neofscrypto.Scheme
		object object.Object
	}{
		{scheme: neofscrypto.ECDSA_SHA512, object: objectECDSASHA512},
		{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: objectECDSARFC6979},
		{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: objectECDSAWalletConnect},
	} {
		require.NoError(t, icrypto.AuthenticateObject(tc.object))
	}
}

// set in init.
var (
	objectECDSASHA512        object.Object
	objectECDSARFC6979       object.Object
	objectECDSAWalletConnect object.Object
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
	par.SetOwner(issuer)
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
	child.SetOwner(issuer)
	child.SetCreationEpoch(creationEpoch)
	child.SetType(typ)
	child.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(childPayload)))
	child.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(childPayload)))
	child.SetSessionToken(&objectSessionECDSASHA512)
	child.SetAttributes(*object.NewAttribute("31429585", "689450942"), *object.NewAttribute("129849325", "859384298"))
	child.SetParent(&par)
	child.SetID(oid.ID{25, 17, 250, 236, 165, 250, 160, 140, 87, 82, 187, 44, 12, 8, 158, 58, 100, 16, 41, 157, 111, 174, 154, 150,
		232, 233, 226, 172, 238, 99, 141, 247})

	return child
}

func init() {
	objectECDSASHA512 = getUnsignedObject()
	objectECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA, []byte{
		4, 120, 189, 133, 160, 231, 85, 45, 168, 156, 247, 131, 90, 93, 201, 80, 135, 207, 211, 4, 181, 153, 60, 59, 125, 134, 10, 176,
		42, 211, 225, 114, 11, 148, 215, 152, 237, 37, 67, 172, 191, 210, 254, 104, 66, 140, 25, 27, 60, 63, 150, 185, 253, 238, 17,
		124, 147, 228, 53, 117, 177, 0, 2, 76, 52,
	})
	objectECDSASHA512.SetSignature(&objectECDSASHA512Sig)

	objectECDSARFC6979 = getUnsignedObject()
	objectECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA, []byte{
		107, 253, 17, 84, 1, 224, 98, 101, 53, 167, 26, 89, 223, 49, 127, 98, 167, 187, 83, 0, 254, 50, 1, 155, 25, 96, 247, 197, 44,
		65, 81, 71, 86, 248, 232, 234, 140, 157, 75, 111, 205, 226, 86, 236, 119, 67, 174, 242, 107, 239, 51, 161, 190, 46, 47, 106,
		125, 187, 139, 136, 157, 13, 155, 226,
	})
	objectECDSARFC6979.SetSignature(&objectECDSARFC6979Sig)

	objectECDSAWalletConnect = getUnsignedObject()
	objectECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA, []byte{
		84, 5, 202, 158, 44, 80, 124, 72, 23, 229, 165, 239, 50, 132, 58, 19, 38, 11, 103, 133, 9, 72, 247, 40, 181, 106, 4, 127,
		33, 7, 208, 247, 38, 4, 68, 35, 143, 94, 113, 45, 183, 33, 111, 214, 194, 132, 219, 152, 198, 52, 91, 63, 20, 228, 177, 38,
		13, 118, 175, 115, 27, 196, 106, 86, 76, 12, 50, 243, 225, 202, 234, 92, 207, 59, 179, 225, 148, 35, 212, 200,
	})
	objectECDSAWalletConnect.SetSignature(&objectECDSAWalletConnectSig)
}
