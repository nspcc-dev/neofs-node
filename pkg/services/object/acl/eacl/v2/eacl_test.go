package v2

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
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

	obj := object.New()

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
