package v2

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
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
	require.True(s.t, addr.Container().Equals(s.expAddr.Container()))
	require.True(s.t, addr.Object().Equals(s.expAddr.Object()))

	return s.obj, s.err
}

func testXHeaders(strs ...string) []session.XHeader {
	res := make([]session.XHeader, len(strs)/2)

	for i := 0; i < len(strs); i += 2 {
		res[i/2].SetKey(strs[i])
		res[i/2].SetValue(strs[i+1])
	}

	return res
}

func TestHeadRequest(t *testing.T) {
	req := new(objectV2.HeadRequest)

	meta := new(session.RequestMetaHeader)
	req.SetMetaHeader(meta)

	body := new(objectV2.HeadRequestBody)
	req.SetBody(body)

	addr := oidtest.Address()

	var addrV2 refs.Address
	addr.WriteToV2(&addrV2)

	body.SetAddress(&addrV2)

	xKey := "x-key"
	xVal := "x-val"
	xHdrs := testXHeaders(
		xKey, xVal,
	)

	meta.SetXHeaders(xHdrs)

	obj := object.New()

	attrKey := "attr_key"
	attrVal := "attr_val"
	var attr object.Attribute
	attr.SetKey(attrKey)
	attr.SetValue(attrVal)
	obj.SetAttributes(attr)

	table := new(eaclSDK.Table)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	senderKey := priv.PublicKey()

	r := eaclSDK.NewRecord()
	r.SetOperation(eaclSDK.OperationHead)
	r.SetAction(eaclSDK.ActionDeny)
	r.AddFilter(eaclSDK.HeaderFromObject, eaclSDK.MatchStringEqual, attrKey, attrVal)
	r.AddFilter(eaclSDK.HeaderFromRequest, eaclSDK.MatchStringEqual, xKey, xVal)
	eaclSDK.AddFormedTarget(r, eaclSDK.RoleUnknown, (ecdsa.PublicKey)(*senderKey))

	table.AddRecord(r)

	lStorage := &testLocalStorage{
		t:       t,
		expAddr: addr,
		obj:     obj,
	}

	id := addr.Object()

	newSource := func(t *testing.T) eaclSDK.TypedHeaderSource {
		hdrSrc, err := NewMessageHeaderSource(
			WithObjectStorage(lStorage),
			WithServiceRequest(req),
			WithCID(addr.Container()),
			WithOID(&id))
		require.NoError(t, err)
		return hdrSrc
	}

	cnr := addr.Container()

	unit := new(eaclSDK.ValidationUnit).
		WithContainerID(&cnr).
		WithOperation(eaclSDK.OperationHead).
		WithSenderKey(senderKey.Bytes()).
		WithEACLTable(table)

	validator := eaclSDK.NewValidator()

	checkAction(t, eaclSDK.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))

	meta.SetXHeaders(nil)

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	meta.SetXHeaders(xHdrs)

	obj.SetAttributes()

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	lStorage.err = errors.New("any error")

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	r.SetAction(eaclSDK.ActionAllow)

	rID := eaclSDK.NewRecord()
	rID.SetOperation(eaclSDK.OperationHead)
	rID.SetAction(eaclSDK.ActionDeny)
	rID.AddObjectIDFilter(eaclSDK.MatchStringEqual, addr.Object())
	eaclSDK.AddFormedTarget(rID, eaclSDK.RoleUnknown, (ecdsa.PublicKey)(*senderKey))

	table = eaclSDK.NewTable()
	table.AddRecord(r)
	table.AddRecord(rID)

	unit.WithEACLTable(table)
	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))
}

func checkAction(t *testing.T, expected eaclSDK.Action, v *eaclSDK.Validator, u *eaclSDK.ValidationUnit) {
	actual, fromRule := v.CalculateAction(u)
	require.True(t, fromRule)
	require.Equal(t, expected, actual)
}

func checkDefaultAction(t *testing.T, v *eaclSDK.Validator, u *eaclSDK.ValidationUnit) {
	actual, fromRule := v.CalculateAction(u)
	require.False(t, fromRule)
	require.Equal(t, eaclSDK.ActionAllow, actual)
}

func TestV2Split(t *testing.T) {
	attrKey := "allow"
	attrVal := "me"

	var restrictedAttr object.Attribute
	restrictedAttr.SetKey(attrKey)
	restrictedAttr.SetValue(attrVal)

	originalObject := objecttest.Object(t)
	originalObject.SetAttributes(restrictedAttr)
	originalObject.SetID(oid.ID{}) // no object ID for an original object in the first object
	originalObject.SetSignature(&neofscrypto.Signature{})

	originalObjectV2 := originalObject.ToV2()

	firstObject := objecttest.Object(t)
	firstObject.SetSplitID(nil) // not V1 split
	firstObject.SetParent(&originalObject)
	require.NoError(t, firstObject.CalculateAndSetID())

	var firstIDV2 refs.ObjectID
	firstID, _ := firstObject.ID()
	firstID.WriteToV2(&firstIDV2)

	splitV2 := new(objectV2.SplitHeader)
	splitV2.SetFirst(&firstIDV2)
	splitV2.SetParentHeader(originalObjectV2.GetHeader())
	headerV2 := new(objectV2.Header)
	headerV2.SetSplit(splitV2)

	objPart := new(objectV2.PutObjectPartInit)
	objPart.SetHeader(headerV2)

	body := new(objectV2.PutRequestBody)
	body.SetObjectPart(objPart)

	meta := new(session.RequestMetaHeader)

	req := new(objectV2.PutRequest)
	req.SetMetaHeader(meta)
	req.SetBody(body)

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	senderKey := priv.PublicKey()

	r := eaclSDK.NewRecord()
	r.SetOperation(eaclSDK.OperationPut)
	r.SetAction(eaclSDK.ActionDeny)
	r.AddFilter(eaclSDK.HeaderFromObject, eaclSDK.MatchStringEqual, attrKey, attrVal)
	eaclSDK.AddFormedTarget(r, eaclSDK.RoleUnknown, (ecdsa.PublicKey)(*senderKey))

	table := new(eaclSDK.Table)
	table.AddRecord(r)

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
		WithEACLTable(table).
		WithSenderKey(senderKey.Bytes())

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
		headerV2.GetSplit().SetParent(nil)

		checkAction(t, eaclSDK.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))
	})

	t.Run("allow cause no restricted attribute found", func(t *testing.T) {
		originalObjectNoRestrictedAttr := objecttest.Object(t)
		originalObjectNoRestrictedAttr.SetID(oid.ID{}) // no object ID for an original object in the first object
		originalObjectNoRestrictedAttr.SetSignature(&neofscrypto.Signature{})

		splitV2.SetParentHeader(originalObjectNoRestrictedAttr.ToV2().GetHeader())

		// allow an object whose first obj does not have the restricted attribute
		checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))
	})
}
