package v2

import (
	"crypto/ecdsa"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type testLocalStorage struct {
	t *testing.T

	expAddr oid.Address

	obj *object.Object
}

func (s *testLocalStorage) Head(addr oid.Address) (*object.Object, error) {
	require.True(s.t, addr.Container().Equals(s.expAddr.Container()))
	require.True(s.t, addr.Object().Equals(s.expAddr.Object()))

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

	require.Equal(t, eaclSDK.ActionDeny, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))

	meta.SetXHeaders(nil)

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))

	meta.SetXHeaders(xHdrs)

	obj.SetAttributes()

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit.WithHeaderSource(newSource(t))))
}
