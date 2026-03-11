package v2

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
)

type testLocalStorage struct {
	t *testing.T

	expAddr oid.Address

	obj *object.Object

	err error
}

type testHeaderSource struct {
	header *object.Object
}

func (t *testHeaderSource) Head(_ oid.Address) (*object.Object, error) {
	return t.header, nil
}

func (s *testLocalStorage) Head(addr oid.Address) (*object.Object, error) {
	require.True(s.t, addr.Container() == s.expAddr.Container())
	require.True(s.t, addr.Object() == s.expAddr.Object())

	return s.obj, s.err
}

func TestHeadRequest(t *testing.T) {
	cnr := cidtest.ID()
	id := oidtest.ID()

	xKey := "x-key"
	xVal := "x-val"
	xHdrs := []*protosession.XHeader{{Key: xKey, Value: xVal}}

	req := &protoobject.HeadRequest{
		Body: &protoobject.HeadRequest_Body{
			Address: &refs.Address{
				ContainerId: cnr.ProtoMessage(),
				ObjectId:    id.ProtoMessage(),
			},
		},
		MetaHeader: &protosession.RequestMetaHeader{
			XHeaders: xHdrs,
		},
	}

	obj := new(object.Object)

	attrKey := "attr_key"
	attrVal := "attr_val"
	var attr object.Attribute
	attr.SetKey(attrKey)
	attr.SetValue(attrVal)
	obj.SetAttributes(attr)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	senderKey := priv.PublicKey()
	userID := user.NewFromECDSAPublicKey((ecdsa.PublicKey)(*senderKey))

	tgt := eaclSDK.NewTargetByAccounts([]user.ID{userID})

	r := eaclSDK.ConstructRecord(
		eaclSDK.ActionDeny,
		eaclSDK.OperationHead,
		[]eaclSDK.Target{tgt},
		eaclSDK.NewObjectPropertyFilter(attrKey, eaclSDK.MatchStringEqual, attrVal),
		eaclSDK.NewRequestHeaderFilter(xKey, eaclSDK.MatchStringEqual, xVal))
	table := eaclSDK.ConstructTable([]eaclSDK.Record{r})

	lStorage := &testLocalStorage{
		t:       t,
		expAddr: oid.NewAddress(cnr, id),
		obj:     obj,
	}

	newSource := func(t *testing.T) eaclSDK.TypedHeaderSource {
		hdrSrc, err := NewMessageHeaderSource(
			WithObjectStorage(lStorage),
			WithServiceRequest(req),
			WithCID(cnr),
			WithOID(&id))
		require.NoError(t, err)
		return hdrSrc
	}

	unit := new(eaclSDK.ValidationUnit).
		WithContainerID(&cnr).
		WithOperation(eaclSDK.OperationHead).
		WithAccount(userID).
		WithEACLTable(&table)

	validator := eaclSDK.NewValidator()

	checkAction(t, eaclSDK.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))

	req.MetaHeader.XHeaders = nil

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	req.MetaHeader.XHeaders = xHdrs

	obj.SetAttributes()

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	lStorage.err = errors.New("any error")

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	r.SetAction(eaclSDK.ActionAllow)

	rID := eaclSDK.ConstructRecord(
		eaclSDK.ActionDeny,
		eaclSDK.OperationHead,
		[]eaclSDK.Target{tgt},
		eaclSDK.NewFilterObjectWithID(id),
	)

	table = eaclSDK.ConstructTable([]eaclSDK.Record{r, rID})

	unit.WithEACLTable(&table)
	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))
}

func checkAction(t *testing.T, expected eaclSDK.Action, v *eaclSDK.Validator, u *eaclSDK.ValidationUnit) {
	actual, fromRule, err := v.CalculateAction(u)
	require.NoError(t, err)
	require.True(t, fromRule)
	require.Equal(t, expected, actual)
}

func checkDefaultAction(t *testing.T, v *eaclSDK.Validator, u *eaclSDK.ValidationUnit) {
	actual, fromRule, err := v.CalculateAction(u)
	require.NoError(t, err)
	require.False(t, fromRule)
	require.Equal(t, eaclSDK.ActionAllow, actual)
}

func TestV2Split(t *testing.T) {
	attrKey := "allow"
	attrVal := "me"

	var restrictedAttr object.Attribute
	restrictedAttr.SetKey(attrKey)
	restrictedAttr.SetValue(attrVal)

	originalObject := objecttest.Object()
	originalObject.SetAttributes(restrictedAttr)
	originalObject.SetID(oid.ID{}) // no object ID for an original object in the first object
	originalObject.SetSignature(&neofscrypto.Signature{})

	firstObject := objecttest.Object()
	firstObject.SetSplitID(nil) // not V1 split
	firstObject.SetParent(&originalObject)
	require.NoError(t, firstObject.CalculateAndSetID())

	hs := &protoobject.Header_Split{
		ParentHeader: originalObject.ProtoMessage().Header,
		First:        firstObject.GetID().ProtoMessage(),
	}
	hdr := &protoobject.Header{
		Split: hs,
	}
	req := &protoobject.PutRequest{
		Body: &protoobject.PutRequest_Body{
			ObjectPart: &protoobject.PutRequest_Body_Init_{Init: &protoobject.PutRequest_Body_Init{
				Header: hdr,
			}},
		},
	}

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	senderKey := priv.PublicKey()
	userID := user.NewFromECDSAPublicKey((ecdsa.PublicKey)(*senderKey))

	tgt := eaclSDK.NewTargetByAccounts([]user.ID{userID})
	r := eaclSDK.ConstructRecord(
		eaclSDK.ActionDeny,
		eaclSDK.OperationPut,
		[]eaclSDK.Target{tgt},
		eaclSDK.NewObjectPropertyFilter(attrKey, eaclSDK.MatchStringEqual, attrVal),
	)

	table := eaclSDK.ConstructTable([]eaclSDK.Record{r})

	hdrSrc := &testHeaderSource{}

	newSource := func(t *testing.T) eaclSDK.TypedHeaderSource {
		hdrSrc, err := NewMessageHeaderSource(
			WithHeaderSource(hdrSrc),
			WithServiceRequest(req),
		)
		require.NoError(t, err)
		return hdrSrc
	}

	unit := new(eaclSDK.ValidationUnit).
		WithOperation(eaclSDK.OperationPut).
		WithEACLTable(&table).
		WithAccount(userID)

	validator := eaclSDK.NewValidator()

	t.Run("denied by parent's attribute; first object", func(t *testing.T) {
		// ensure fetching the first object is not possible, only already attached information
		// is available
		hdrSrc.header = nil

		checkAction(t, eaclSDK.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))
	})

	t.Run("denied by parent's attribute; non-first object", func(t *testing.T) {
		// get the first object from the "network"
		hdrSrc.header = &firstObject

		checkAction(t, eaclSDK.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))
	})

	t.Run("allow cause no restricted attribute found", func(t *testing.T) {
		originalObjectNoRestrictedAttr := objecttest.Object()
		originalObjectNoRestrictedAttr.SetID(oid.ID{}) // no object ID for an original object in the first object
		originalObjectNoRestrictedAttr.SetSignature(&neofscrypto.Signature{})

		hs.ParentHeader = originalObjectNoRestrictedAttr.ProtoMessage().Header

		// allow an object whose first obj does not have the restricted attribute
		checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))
	})
}

func TestObjectHeaders(t *testing.T) {
	id := oid.ID{115, 0, 118, 16, 207, 179, 251, 24, 242, 80, 173, 62, 203, 159, 249, 96, 133, 158, 64, 52, 145, 73, 2, 217, 66, 141, 1, 240, 129, 44, 196, 30}
	const idStr = "8jvJ15BTY16PQ4FVuPs2r8WVHQCdKGBuBt7m3AQCpyMF"
	cnr := cid.ID{220, 109, 68, 96, 19, 33, 31, 137, 22, 248, 206, 34, 29, 21, 216, 65, 22, 28, 132, 248, 155, 146, 160, 16, 154, 143, 67, 147, 156, 117, 183, 82}
	const cnrStr = "FqTL8htsZjuNjH3EXB4G9TJihScYZHmWdYMR3rPBEs3o"
	owner := user.ID{53, 47, 130, 106, 87, 14, 223, 144, 69, 219, 27, 43, 144, 42, 209, 217, 223, 184, 98, 92, 83, 171, 246, 77, 244}
	const ownerStr = "NQFBBC6odu6VULXFmnfP8mucTt1nWDwfmM"
	const payloadLen = 16283534215572835787
	const payloadLenStr = "16283534215572835787"
	const creationEpoch = 11361641039719275900
	const creationEpochStr = "11361641039719275900"
	payloadHash := checksum.NewSHA256([sha256.Size]byte{250, 136, 163, 172, 95, 206, 95, 222, 135, 183, 75, 254, 82, 63, 114, 86, 175, 234, 218, 126, 249, 126, 15, 93, 228, 69, 103, 226, 188, 20, 53, 15})
	const payloadHashStr = "SHA256:fa88a3ac5fce5fde87b74bfe523f7256afeada7ef97e0f5de44567e2bc14350f"
	homoHash := checksum.NewTillichZemor([tz.Size]byte{199, 94, 209, 171, 81, 255, 104, 105, 199, 78, 103, 145, 33, 82, 51, 104, 36, 71, 94, 186, 122, 163, 215, 213, 37, 126, 202, 109, 135, 119, 180, 166, 97, 152, 172,
		81, 109, 199, 85, 214, 251, 33, 77, 105, 107, 117, 10, 177, 172, 16, 236, 68, 168, 250, 78, 238, 247, 11, 4, 175, 194, 125, 131, 30})
	const homoHashStr = "TZ:c75ed1ab51ff6869c74e67912152336824475eba7aa3d7d5257eca6d8777b4a66198ac516dc755d6fb214d696b750ab1ac10ec44a8fa4eeef70b04afc27d831e"

	var obj object.Object
	obj.SetContainerID(cidtest.OtherID(cnr))
	obj.SetID(oidtest.OtherID(id))
	obj.SetPayloadSize(payloadLen)
	obj.SetCreationEpoch(creationEpoch)

	newHeadResponse := func(obj object.Object) *protoobject.HeadResponse {
		return &protoobject.HeadResponse{
			Body: &protoobject.HeadResponse_Body{
				Head: &protoobject.HeadResponse_Body_Header{
					Header: &protoobject.HeaderWithSignature{
						Header: obj.ProtoMessage().Header,
					},
				},
			},
		}
	}

	testWithOption := func(t *testing.T, id *oid.ID, exp [][2]string, opt Option) {
		src, err := NewMessageHeaderSource(
			WithCID(cnr),
			WithOID(id),
			opt,
		)
		require.NoError(t, err)

		hs, rdy, err := src.HeadersOfType(eaclSDK.HeaderFromObject)
		require.NoError(t, err)
		require.True(t, rdy)

		var res [][2]string
		for i := range hs {
			res = append(res, [2]string{hs[i].Key(), hs[i].Value()})
		}

		require.ElementsMatch(t, exp, res)
	}

	testWithObject := func(t *testing.T, id *oid.ID, obj object.Object, exp [][2]string) {
		t.Run("binary", func(t *testing.T) {
			msg := obj.ProtoMessage().Header
			buf := make([]byte, msg.MarshaledSize())
			msg.MarshalStable(buf)

			testWithOption(t, id, exp, WithObjectHeaderBinary(buf))
		})

		testWithOption(t, id, exp, WithServiceResponse(newHeadResponse(obj), nil))
	}

	t.Run("min", func(t *testing.T) {
		testWithObject(t, nil, obj, [][2]string{
			{"$Object:version", "v0.0"},
			{"$Object:containerID", cnrStr},
			{"$Object:creationEpoch", creationEpochStr},
			{"$Object:payloadLength", payloadLenStr},
			{"$Object:objectType", "REGULAR"},
		})
	})

	ver := version.New(12, 34)
	obj.SetVersion(&ver)
	obj.SetOwner(owner)
	obj.SetPayloadChecksum(payloadHash)
	obj.SetPayloadHomomorphicHash(homoHash)
	obj.SetAttributes(
		object.NewAttribute("foo", "bar"),
		object.NewAttribute("hello", "world"),
	)

	t.Run("child", func(t *testing.T) {
		childOwner := user.ID{53, 227, 83, 145, 150, 191, 175, 252, 101, 125, 181, 30, 178, 95, 219, 37, 50, 241, 23, 162, 232, 20, 135, 225, 219}
		const childOwnerStr = "NgdxgkriuPkiXLfhehs2odWxj3VAWh1qQ6"
		const childCreationEpoch = 8631081124895577949
		const childCreationEpochStr = "8631081124895577949"
		const childPayloadLen = 17034386584505117111
		const childPayloadLenStr = "17034386584505117111"
		childPayloadHash := checksum.NewSHA256([sha256.Size]byte{70, 249, 108, 82, 211, 20, 129, 201, 177, 205, 253, 190, 156, 148, 7, 122, 165, 153, 54, 128, 96, 0, 246, 232, 5, 78, 111, 190, 68, 74, 167, 174})
		const childPayloadHashStr = "SHA256:46f96c52d31481c9b1cdfdbe9c94077aa59936806000f6e8054e6fbe444aa7ae"
		childHomoHash := checksum.NewTillichZemor([tz.Size]byte{191, 33, 176, 230, 72, 226, 40, 54, 248, 99, 98, 74, 97, 223, 128, 145, 184, 14, 124, 85, 113, 204, 145, 55, 214, 96, 210, 161, 27, 170, 203, 191,
			246, 83, 173, 163, 112, 152, 185, 131, 251, 218, 64, 145, 78, 29, 203, 105, 12, 150, 123, 54, 109, 23, 29, 140, 87, 196, 70, 81, 88, 110, 152, 99})
		const childHomoHashStr = "TZ:bf21b0e648e22836f863624a61df8091b80e7c5571cc9137d660d2a11baacbbff653ada37098b983fbda40914e1dcb690c967b366d171d8c57c44651586e9863"

		var child object.Object
		child.SetContainerID(cidtest.OtherID(cnr))
		child.SetID(oidtest.OtherID(id))
		child.SetOwner(childOwner)
		child.SetPayloadSize(childPayloadLen)
		child.SetCreationEpoch(childCreationEpoch)
		ver := version.New(56, 78)
		child.SetVersion(&ver)
		child.SetType(object.TypeLock)
		child.SetPayloadChecksum(childPayloadHash)
		child.SetPayloadHomomorphicHash(childHomoHash)
		child.SetParent(&obj)

		testWithObject(t, &id, child, [][2]string{
			{"$Object:objectID", idStr},
			{"$Object:objectID", idStr}, // redundantly repeated
			{"$Object:version", "v12.34"},
			{"$Object:version", "v56.78"},
			{"$Object:containerID", cnrStr},
			{"$Object:containerID", cnrStr}, // redundantly repeated
			{"$Object:ownerID", ownerStr},
			{"$Object:ownerID", childOwnerStr},
			{"$Object:creationEpoch", creationEpochStr},
			{"$Object:creationEpoch", childCreationEpochStr},
			{"$Object:payloadLength", payloadLenStr},
			{"$Object:payloadLength", childPayloadLenStr},
			{"$Object:objectType", "REGULAR"},
			{"$Object:objectType", "LOCK"},
			{"$Object:payloadHash", payloadHashStr},
			{"$Object:payloadHash", childPayloadHashStr},
			{"$Object:homomorphicHash", homoHashStr},
			{"$Object:homomorphicHash", childHomoHashStr},
			{"foo", "bar"},
			{"hello", "world"},
		})
	})

	testWithObject(t, &id, obj, [][2]string{
		{"$Object:objectID", idStr},
		{"$Object:version", "v12.34"},
		{"$Object:containerID", cnrStr},
		{"$Object:ownerID", ownerStr},
		{"$Object:creationEpoch", creationEpochStr},
		{"$Object:payloadLength", payloadLenStr},
		{"$Object:objectType", "REGULAR"},
		{"$Object:payloadHash", payloadHashStr},
		{"$Object:homomorphicHash", homoHashStr},
		{"foo", "bar"},
		{"hello", "world"},
	})
}
