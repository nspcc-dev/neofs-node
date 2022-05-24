package v2

import (
	"crypto/ecdsa"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	"github.com/stretchr/testify/require"
)

type testLocalStorage struct {
	t *testing.T

	expAddr *objectSDKAddress.Address

	obj *object.Object
}

func (s *testLocalStorage) Head(addr *objectSDKAddress.Address) (*object.Object, error) {
	cnr1, ok := s.expAddr.ContainerID()
	require.True(s.t, ok)

	cnr2, ok := addr.ContainerID()
	require.True(s.t, ok)

	require.True(s.t, cnr1.Equals(cnr2))

	id1, ok := s.expAddr.ObjectID()
	require.True(s.t, ok)

	id2, ok := addr.ObjectID()
	require.True(s.t, ok)

	require.True(s.t, id1.Equals(id2))

	return s.obj, nil
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

	addr := objecttest.Address()
	body.SetAddress(addr.ToV2())

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

	cid, _ := addr.ContainerID()
	oid, _ := addr.ObjectID()

	newSource := func(t *testing.T) eaclSDK.TypedHeaderSource {
		hdrSrc, err := NewMessageHeaderSource(
			WithObjectStorage(lStorage),
			WithServiceRequest(req),
			WithCID(cid),
			WithOID(&oid))
		require.NoError(t, err)
		return hdrSrc
	}

	cnr, _ := addr.ContainerID()
	unit := new(eaclSDK.ValidationUnit).
		WithContainerID(&cnr).
		WithOperation(eaclSDK.OperationHead).
		WithSenderKey(senderKey.Bytes()).
		WithEACLTable(table)

	validator := eaclSDK.NewValidator()

	require.Equal(t, eaclSDK.ActionDeny, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))

	meta.SetXHeaders(nil)

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))

	meta.SetXHeaders(xHdrs)

	obj.SetAttributes()

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))
}
