package object

import (
	"context"
	"slices"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

func blankValidObject(signer user.Signer) *object.Object {
	obj := object.New()
	obj.SetContainerID(cidtest.ID())
	obj.SetOwner(signer.UserID())

	return obj
}

// returns an object which is valid when signed by the resulting signer.
func minUnsignedObject(t testing.TB) (object.Object, usertest.UserSigner) {
	usr := usertest.User()

	var obj object.Object
	obj.SetContainerID(cidtest.ID())
	obj.SetOwner(usr.ID)
	require.NoError(t, obj.CalculateAndSetID())

	return obj, usr
}

type testNetState struct {
	epoch uint64
}

func (s testNetState) CurrentEpoch() uint64 {
	return s.epoch
}

type testLockSource struct {
	m map[oid.Address]bool
}

func (t testLockSource) IsLocked(address oid.Address) (bool, error) {
	return t.m[address], nil
}

type tombstoneVerifier struct {
}

func (t2 tombstoneVerifier) VerifyTomb(_ context.Context, _ cid.ID, _ object.Tombstone) error {
	return nil
}

func TestFormatValidator_Validate(t *testing.T) {
	const curEpoch = 13

	ls := testLockSource{
		m: make(map[oid.Address]bool),
	}

	v := NewFormatValidator(
		WithNetState(testNetState{
			epoch: curEpoch,
		}),
		WithLockSource(ls),
		WithTombVerifier(tombstoneVerifier{}),
	)

	ownerKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	t.Run("nil input", func(t *testing.T) {
		require.Error(t, v.Validate(nil, true))
	})

	t.Run("invalid identifier", func(t *testing.T) {
		t.Run("missing", func(t *testing.T) {
			obj := smallECDSASHA512
			obj.ResetID()
			require.ErrorIs(t, v.Validate(&obj, false), errNilID)
		})
		t.Run("wrong", func(t *testing.T) {
			require.EqualError(t, v.Validate(&wrongIDECDSASHA512, false), "could not validate header fields: invalid identifier: incorrect object identifier")
		})
	})

	t.Run("nil container identifier", func(t *testing.T) {
		obj := object.New()
		obj.SetID(oidtest.ID())

		require.ErrorIs(t, v.Validate(obj, true), errNilCID)
	})

	t.Run("invalid signature", func(t *testing.T) {
		t.Run("unsigned", func(t *testing.T) {
			obj, _ := minUnsignedObject(t)
			require.EqualError(t, v.Validate(&obj, false), "could not validate signature: missing signature")
		})
		t.Run("unsupported scheme", func(t *testing.T) {
			obj, signer := minUnsignedObject(t)

			sigBytes, err := signer.Sign(obj.GetID().Marshal())
			require.NoError(t, err)
			sig := neofscrypto.NewSignature(3, signer.Public(), sigBytes)
			obj.SetSignature(&sig)

			require.EqualError(t, v.Validate(&obj, false), "could not validate signature: invalid signature")
		})
		t.Run("wrong scheme", func(t *testing.T) {
			obj, signer := minUnsignedObject(t)

			sigBytes, err := signer.RFC6979.Sign(obj.GetID().Marshal())
			require.NoError(t, err)
			sig := neofscrypto.NewSignature(neofscrypto.ECDSA_WALLETCONNECT, signer.Public(), sigBytes)
			obj.SetSignature(&sig)

			require.EqualError(t, v.Validate(&obj, false), "could not validate signature: invalid signature")
		})
		t.Run("invalid public key", func(t *testing.T) {
			obj, signer := minUnsignedObject(t)
			sigBytes, err := signer.Sign(obj.GetID().Marshal())
			require.NoError(t, err)
			sig := neofscrypto.NewSignatureFromRawKey(signer.Scheme(), signer.PublicKeyBytes, sigBytes)

			for _, tc := range []struct {
				name      string
				changePub func([]byte) []byte
				skip      bool
			}{
				{name: "nil", changePub: func([]byte) []byte { return nil }},
				{name: "empty", changePub: func([]byte) []byte { return []byte{} }},
				{name: "undersize", changePub: func(k []byte) []byte { return k[:len(k)-1] }},
				{name: "oversize", changePub: func(k []byte) []byte { return append(k, 1) }},
				{name: "prefix 0", changePub: func(k []byte) []byte { return []byte{0x00} }, skip: true},
				{name: "prefix 1", changePub: func(k []byte) []byte { return []byte{0x01} }},
				{name: "prefix 4", changePub: func(k []byte) []byte { return []byte{0x04} }},
				{name: "prefix 5", changePub: func(k []byte) []byte { return []byte{0x05} }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.skip {
						t.Skip()
					}
					pub := slices.Clone(signer.PublicKeyBytes)
					sig.SetPublicKeyBytes(tc.changePub(pub))
					obj.SetSignature(&sig)
					require.EqualError(t, v.Validate(&obj, false), "could not validate signature: invalid signature")
				})
			}
		})
		t.Run("mismatch", func(t *testing.T) {
			for _, tc := range []struct {
				scheme neofscrypto.Scheme
				object object.Object
			}{
				{scheme: neofscrypto.ECDSA_SHA512, object: smallECDSASHA512},
				{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: smallECDSARFC6979},
				{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: smallECDSAWalletConnect},
			} {
				t.Run(tc.scheme.String(), func(t *testing.T) {
					sig := tc.object.Signature()
					require.NotNil(t, sig)
					validSig := sig.Value()
					for i := range validSig {
						cp := slices.Clone(validSig)
						cp[i]++
						newSig := neofscrypto.NewSignatureFromRawKey(sig.Scheme(), sig.PublicKeyBytes(), cp)
						tc.object.SetSignature(&newSig)
						require.EqualError(t, v.Validate(&tc.object, false), "could not validate signature: invalid signature")
					}
				})
			}
		})
	})

	t.Run("correct w/ session token", func(t *testing.T) {
		signer := user.NewAutoIDSignerRFC6979(ownerKey.PrivateKey)

		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		tok := sessiontest.Object()
		tok.SetAuthKey((*neofsecdsa.PublicKey)(&ownerKey.PrivateKey.PublicKey))
		require.NoError(t, tok.Sign(signer))
		obj.SetSessionToken(&tok)
		obj.SetOwner(signer.UserID())

		require.NoError(t, obj.SetIDWithSignature(signer))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("incorrect session token", func(t *testing.T) {
		signer := user.NewAutoIDSignerRFC6979(ownerKey.PrivateKey)

		obj := object.New()
		obj.SetContainerID(cidtest.ID())
		tok := sessiontest.ObjectSigned(signer)
		obj.SetSessionToken(&tok)
		obj.SetOwner(signer.UserID())

		t.Run("wrong signature", func(t *testing.T) {
			require.NoError(t, obj.SetIDWithSignature(signer))

			obj.SetSignature(&neofscrypto.Signature{})
			require.Error(t, v.Validate(obj, false))
		})

		t.Run("wrong owner", func(t *testing.T) {
			obj.SetOwner(user.ID{})

			require.NoError(t, obj.SetIDWithSignature(signer))
			require.Error(t, v.Validate(obj, false))
		})

		t.Run("wrong signer", func(t *testing.T) {
			wrongOwner, err := keys.NewPrivateKey()
			require.NoError(t, err)

			wrongSigner := user.NewAutoIDSignerRFC6979(wrongOwner.PrivateKey)

			require.NoError(t, obj.SetIDWithSignature(wrongSigner))
			require.Error(t, v.Validate(obj, false))
		})

		t.Run("signed not by issuer", func(t *testing.T) {
			sessionSubj := usertest.User()
			otherUser := usertest.User()

			var st session.Object
			st.SetAuthKey(sessionSubj.Public())
			st.SetIssuer(sessionSubj.ID)

			sig, err := otherUser.Sign(st.SignedData())
			require.NoError(t, err)
			st.AttachSignature(neofscrypto.NewSignature(otherUser.Scheme(), otherUser.Public(), sig))

			obj := blankValidObject(sessionSubj)
			obj.SetSessionToken(&st)
			require.NoError(t, obj.CalculateAndSetID())
			require.NoError(t, obj.Sign(sessionSubj))

			err = v.Validate(obj, false)
			require.EqualError(t, err, "could not validate signature: authenticate session token: issuer mismatches signature")
		})
	})

	t.Run("correct w/o session token", func(t *testing.T) {
		signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
		obj := blankValidObject(signer)

		require.NoError(t, obj.SetIDWithSignature(signer))

		require.NoError(t, v.Validate(obj, false))
	})

	t.Run("tombstone content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeTombstone)
		obj.SetContainerID(cidtest.ID())

		_, err := v.ValidateContent(obj)
		require.Error(t, err) // no tombstone content

		content := object.NewTombstone()
		content.SetMembers([]oid.ID{oidtest.ID()})

		obj.SetPayload(content.Marshal())

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // no members in tombstone

		content.SetMembers([]oid.ID{oidtest.ID()})

		obj.SetPayload(content.Marshal())

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // no expiration epoch in tombstone

		var expirationAttribute object.Attribute
		expirationAttribute.SetKey(object.AttributeExpirationEpoch)
		expirationAttribute.SetValue(strconv.Itoa(10))

		obj.SetAttributes(expirationAttribute)

		_, err = v.ValidateContent(obj)
		require.Error(t, err) // different expiration values

		id := oidtest.ID()

		content.SetExpirationEpoch(10)
		content.SetMembers([]oid.ID{id})

		obj.SetPayload(content.Marshal())

		contentGot, err := v.ValidateContent(obj)
		require.NoError(t, err) // all good

		require.EqualValues(t, []oid.ID{id}, contentGot.Objects())
		require.Equal(t, object.TypeTombstone, contentGot.Type())
	})

	t.Run("storage group content", func(t *testing.T) {
		obj := object.New()
		obj.SetType(object.TypeStorageGroup)

		t.Run("empty payload", func(t *testing.T) {
			_, err := v.ValidateContent(obj)
			require.Error(t, err)
		})

		var content storagegroup.StorageGroup
		content.SetValidationDataSize(1) // some non-default value

		t.Run("empty members", func(t *testing.T) {
			obj.SetPayload(content.Marshal())

			_, err = v.ValidateContent(obj)
			require.ErrorIs(t, err, errEmptySGMembers)
		})

		t.Run("non-unique members", func(t *testing.T) {
			id := oidtest.ID()

			content.SetMembers([]oid.ID{id, id})

			obj.SetPayload(content.Marshal())

			_, err = v.ValidateContent(obj)
			require.Error(t, err)
		})

		t.Run("correct SG", func(t *testing.T) {
			ids := []oid.ID{oidtest.ID(), oidtest.ID()}
			content.SetMembers(ids)

			obj.SetPayload(content.Marshal())

			content, err := v.ValidateContent(obj)
			require.NoError(t, err)

			require.EqualValues(t, ids, content.Objects())
			require.Equal(t, object.TypeStorageGroup, content.Type())
		})
	})

	t.Run("expiration", func(t *testing.T) {
		fn := func(val string) *object.Object {
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

			var a object.Attribute
			a.SetKey(object.AttributeExpirationEpoch)
			a.SetValue(val)

			obj.SetAttributes(a)

			require.NoError(t, obj.SetIDWithSignature(signer))

			return obj
		}

		t.Run("invalid attribute value", func(t *testing.T) {
			val := "text"
			err := v.Validate(fn(val), false)
			require.Error(t, err)
		})

		t.Run("expired object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch-1, 10)
			obj := fn(val)

			t.Run("non-locked", func(t *testing.T) {
				err := v.Validate(obj, false)
				require.ErrorIs(t, err, errExpired)
			})

			t.Run("locked", func(t *testing.T) {
				var addr oid.Address
				oID := obj.GetID()
				cID := obj.GetContainerID()

				addr.SetContainer(cID)
				addr.SetObject(oID)
				ls.m[addr] = true

				err := v.Validate(obj, false)
				require.NoError(t, err)
			})
		})

		t.Run("alive object", func(t *testing.T) {
			val := strconv.FormatUint(curEpoch, 10)
			err := v.Validate(fn(val), true)
			require.NoError(t, err)
		})
	})

	t.Run("attributes", func(t *testing.T) {
		t.Run("duplication", func(t *testing.T) {
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

			var a1 object.Attribute
			a1.SetKey("key1")
			a1.SetValue("val1")

			var a2 object.Attribute
			a2.SetKey("key2")
			a2.SetValue("val2")

			obj.SetAttributes(a1, a2)

			err := v.checkAttributes(obj)
			require.NoError(t, err)

			a2.SetKey(a1.Key())
			obj.SetAttributes(a1, a2)

			err = v.checkAttributes(obj)
			require.Equal(t, errDuplAttr, err)
		})

		t.Run("empty value", func(t *testing.T) {
			signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
			obj := blankValidObject(signer)

			var a object.Attribute
			a.SetKey("key")

			obj.SetAttributes(a)

			err := v.checkAttributes(obj)
			require.Equal(t, errEmptyAttrVal, err)
		})

		t.Run("zero byte", func(t *testing.T) {
			objWithAttr := func(k, v string) *object.Object {
				obj := blankValidObject(usertest.User())
				obj.SetAttributes(
					*object.NewAttribute("valid key", "valid value"),
					*object.NewAttribute(k, v),
				)
				return obj
			}
			t.Run("in key", func(t *testing.T) {
				obj := objWithAttr("k\x00y", "value")
				require.EqualError(t, v.Validate(obj, true), "invalid attributes: invalid attribute #1: invalid key: illegal zero byte")
				obj.SetID(oidtest.ID())
				require.EqualError(t, v.Validate(obj, false), "invalid attributes: invalid attribute #1: invalid key: illegal zero byte")
			})
			t.Run("in value", func(t *testing.T) {
				obj := objWithAttr("key", "va\x00ue")
				require.EqualError(t, v.Validate(obj, true), "invalid attributes: invalid attribute #1: invalid value: illegal zero byte")
				obj.SetID(oidtest.ID())
				require.EqualError(t, v.Validate(obj, false), "invalid attributes: invalid attribute #1: invalid value: illegal zero byte")
			})
		})
	})
	for _, tc := range []struct {
		scheme neofscrypto.Scheme
		object object.Object
	}{
		{scheme: neofscrypto.ECDSA_SHA512, object: smallECDSASHA512},
		{scheme: neofscrypto.ECDSA_DETERMINISTIC_SHA256, object: smallECDSARFC6979},
		{scheme: neofscrypto.ECDSA_WALLETCONNECT, object: smallECDSAWalletConnect},
	} {
		t.Run(tc.scheme.String(), func(t *testing.T) {
			require.NoError(t, v.Validate(&tc.object, false))
		})
	}
}

type testSplitVerifier struct {
}

func (t *testSplitVerifier) VerifySplit(ctx context.Context, cID cid.ID, firstID oid.ID, children []object.MeasuredObject) error {
	return nil
}

func TestLinkObjectSplitV2(t *testing.T) {
	verifier := new(testSplitVerifier)

	v := NewFormatValidator(
		WithSplitVerifier(verifier),
	)

	ownerKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	signer := user.NewAutoIDSigner(ownerKey.PrivateKey)
	obj := blankValidObject(signer)
	obj.SetParent(object.New())
	obj.SetSplitID(object.NewSplitID())
	obj.SetFirstID(oidtest.ID())

	t.Run("V1 split, first is set", func(t *testing.T) {
		require.ErrorContains(t, v.Validate(obj, true), "first object ID is set")
	})

	t.Run("V2 split", func(t *testing.T) {
		t.Run("link object without finished parent", func(t *testing.T) {
			obj.ResetRelations()
			obj.SetParent(object.New())
			obj.SetFirstID(oidtest.ID())
			obj.SetType(object.TypeLink)

			require.ErrorContains(t, v.Validate(obj, true), "incorrect link object's parent header")
		})

		t.Run("middle child does not have previous object ID", func(t *testing.T) {
			obj.ResetRelations()
			obj.SetParent(object.New())
			obj.SetFirstID(oidtest.ID())
			obj.SetType(object.TypeRegular)

			require.ErrorContains(t, v.Validate(obj, true), "middle part does not have previous object ID")
		})
	})
}

// set in init.
var (
	smallECDSASHA512        object.Object
	smallECDSARFC6979       object.Object
	smallECDSAWalletConnect object.Object

	wrongIDECDSASHA512 object.Object
)

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
	ownerECDSA = user.ID{53, 57, 243, 96, 136, 255, 217, 227, 204, 13, 243, 228, 109, 31, 226, 226, 236, 62, 13, 190, 156, 135, 252, 236, 8}
)

func getUnsignedObject() object.Object {
	const typ = 523555632
	const creationEpoch = 10734578609111205341
	ver := version.New(123, 456)
	cnr := cid.ID{61, 208, 16, 128, 106, 78, 90, 196, 156, 65, 180, 142, 62, 137, 245, 242, 69, 250, 212, 176, 35, 114, 239, 114, 53,
		231, 19, 14, 46, 67, 163, 155}
	payload := []byte("Hello, world!")
	payloadLen := uint64(len(payload))

	var obj object.Object
	obj.SetVersion(&ver)
	obj.SetContainerID(cnr)
	obj.SetOwner(ownerECDSA)
	obj.SetCreationEpoch(creationEpoch)
	obj.SetPayloadSize(payloadLen)
	obj.SetType(typ)
	obj.SetAttributes(
		*object.NewAttribute("2958922649728181678", "13190161871171818625"),
		*object.NewAttribute("15660925904602412174", "67216741819241411480"),
	)

	obj.SetPayload(payload)
	obj.CalculateAndSetPayloadChecksum()
	obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(payload)))

	obj.SetID(oid.ID{231, 249, 6, 213, 114, 154, 74, 74, 49, 179, 107, 109, 34, 56, 68, 54, 226, 226, 16, 54, 217, 41, 138, 188, 245,
		97, 133, 227, 199, 159, 163, 21}) // header checksum

	return obj
}

func init() {
	smallECDSASHA512 = getUnsignedObject()
	smallECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA, []byte{
		4, 155, 202, 63, 77, 61, 145, 168, 242, 244, 233, 217, 104, 177, 168, 238, 60, 245, 248, 110, 173, 244, 253, 25, 106, 241,
		155, 255, 146, 133, 58, 142, 37, 252, 48, 246, 71, 183, 118, 139, 122, 51, 167, 225, 52, 172, 144, 76, 79, 208, 210, 66, 118,
		141, 198, 206, 220, 180, 57, 5, 105, 179, 58, 21, 255,
	})
	smallECDSASHA512.SetSignature(&smallECDSASHA512Sig)

	smallECDSARFC6979 = getUnsignedObject()
	smallECDSARFC6979Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, pubECDSA, []byte{
		86, 187, 47, 50, 36, 253, 213, 202, 229, 147, 51, 192, 54, 230, 52, 223, 69, 50, 136, 141, 103, 119, 72, 251, 86, 44, 224,
		46, 3, 225, 37, 71, 43, 190, 33, 99, 64, 218, 92, 80, 215, 40, 144, 165, 208, 139, 51, 212, 223, 109, 82, 209, 167, 66, 147,
		198, 168, 41, 160, 93, 93, 30, 195, 230,
	})
	smallECDSARFC6979.SetSignature(&smallECDSARFC6979Sig)

	smallECDSAWalletConnect = getUnsignedObject()
	smallECDSAWalletConnectSig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_WALLETCONNECT, pubECDSA, []byte{
		226, 1, 51, 87, 97, 60, 78, 234, 198, 247, 158, 205, 143, 163, 146, 100, 46, 146, 77, 86, 91, 102, 74, 235, 138, 12, 46, 59, 33,
		166, 149, 7, 185, 247, 253, 2, 12, 45, 33, 53, 59, 200, 255, 137, 115, 101, 29, 165, 127, 14, 178, 130, 34, 43, 65, 232, 9, 180,
		164, 37, 91, 103, 137, 183, 250, 198, 162, 23, 131, 43, 36, 243, 142, 150, 255, 142, 219, 254, 19, 130,
	})
	smallECDSAWalletConnect.SetSignature(&smallECDSAWalletConnectSig)

	wrongIDECDSASHA512 = getUnsignedObject()
	wrongIDECDSASHA512.SetID(oid.ID{5, 104, 41, 144, 139, 252, 213, 27, 84, 108, 178, 104, 182, 49, 128, 141, 144, 15, 139, 244, 204,
		2, 226, 103, 252, 111, 14, 47, 71, 26, 84, 74})
	wrongIDECDSASHA512Sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_SHA512, pubECDSA, []byte{
		4, 92, 245, 104, 184, 113, 204, 219, 76, 50, 55, 231, 229, 25, 199, 232, 194, 230, 88, 219, 156, 61, 221, 243, 74, 88, 150,
		85, 95, 199, 11, 131, 175, 86, 51, 244, 175, 158, 60, 199, 156, 162, 222, 54, 26, 244, 39, 18, 169, 149, 139, 15, 210, 215, 34, 0,
		12, 176, 88, 229, 163, 246, 39, 240, 109,
	})
	wrongIDECDSASHA512.SetSignature(&wrongIDECDSASHA512Sig)
}
