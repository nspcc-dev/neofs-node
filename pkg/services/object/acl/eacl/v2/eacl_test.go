package v2

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

type testLocalStorage struct {
	t *testing.T

	expAddr *objectSDK.Address

	obj *object.Object
}

func (s *testLocalStorage) Head(addr *objectSDK.Address) (*object.Object, error) {
	require.True(s.t, addr.ContainerID().Equal(addr.ContainerID()) && addr.ObjectID().Equal(addr.ObjectID()))

	return s.obj, nil
}

func testID(t *testing.T) *objectSDK.ID {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func testAddress(t *testing.T) *objectSDK.Address {
	addr := objectSDK.NewAddress()
	addr.SetObjectID(testID(t))
	addr.SetContainerID(cidtest.ID())

	return addr
}

func testXHeaders(strs ...string) []*session.XHeader {
	res := make([]*session.XHeader, 0, len(strs)/2)

	for i := 0; i < len(strs); i += 2 {
		x := new(session.XHeader)
		x.SetKey(strs[i])
		x.SetValue(strs[i+1])

		res = append(res, x)
	}

	return res
}

func TestHeadRequest(t *testing.T) {
	req := new(objectV2.HeadRequest)

	meta := new(session.RequestMetaHeader)
	req.SetMetaHeader(meta)

	body := new(objectV2.HeadRequestBody)
	req.SetBody(body)

	addr := testAddress(t)
	body.SetAddress(addr.ToV2())

	xKey := "x-key"
	xVal := "x-val"
	xHdrs := testXHeaders(
		xKey, xVal,
	)

	meta.SetXHeaders(xHdrs)

	obj := object.NewRaw()

	attrKey := "attr_key"
	attrVal := "attr_val"
	attr := objectSDK.NewAttribute()
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
		obj:     obj.Object(),
	}

	cid := addr.ContainerID()
	unit := new(eaclSDK.ValidationUnit).
		WithContainerID(cid).
		WithOperation(eaclSDK.OperationHead).
		WithSenderKey(senderKey.Bytes()).
		WithHeaderSource(
			NewMessageHeaderSource(
				WithObjectStorage(lStorage),
				WithServiceRequest(req),
			),
		).
		WithEACLTable(table)

	validator := eaclSDK.NewValidator()

	require.Equal(t, eaclSDK.ActionDeny, validator.CalculateAction(unit))

	meta.SetXHeaders(nil)

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit))

	meta.SetXHeaders(xHdrs)

	obj.SetAttributes(nil)

	require.Equal(t, eaclSDK.ActionAllow, validator.CalculateAction(unit))
}
