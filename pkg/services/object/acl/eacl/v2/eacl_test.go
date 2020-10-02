package v2

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	eacl2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/stretchr/testify/require"
)

type testLocalStorage struct {
	t *testing.T

	expAddr *objectSDK.Address

	obj *object.Object
}

type testEACLStorage struct {
	t *testing.T

	expCID *container.ID

	table *eacl.Table
}

func (s *testEACLStorage) GetEACL(id *container.ID) (*eacl.Table, error) {
	require.True(s.t, s.expCID.Equal(id))

	return s.table, nil
}

func (s *testLocalStorage) Head(addr *objectSDK.Address) (*object.Object, error) {
	require.True(s.t, addr.GetContainerID().Equal(addr.GetContainerID()) && addr.GetObjectID().Equal(addr.GetObjectID()))

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

func testCID(t *testing.T) *container.ID {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	id := container.NewID()
	id.SetSHA256(cs)

	return id
}

func testAddress(t *testing.T) *objectSDK.Address {
	addr := objectSDK.NewAddress()
	addr.SetObjectID(testID(t))
	addr.SetContainerID(testCID(t))

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

	table := new(eacl.Table)

	senderKey := test.DecodeKey(-1).PublicKey

	r := new(eacl.Record)
	r.SetOperation(eacl.OperationHead)
	r.SetAction(eacl.ActionDeny)
	r.AddFilter(eacl.HeaderFromObject, eacl.MatchStringEqual, attrKey, attrVal)
	r.AddFilter(eacl.HeaderFromRequest, eacl.MatchStringEqual, xKey, xVal)
	r.AddTarget(eacl.RoleUnknown, senderKey)

	table.AddRecord(r)

	lStorage := &testLocalStorage{
		t:       t,
		expAddr: addr,
		obj:     obj.Object(),
	}

	cid := addr.GetContainerID()
	unit := new(eacl2.ValidationUnit).
		WithContainerID(cid).
		WithOperation(eacl.OperationHead).
		WithSenderKey(crypto.MarshalPublicKey(&senderKey)).
		WithHeaderSource(
			NewMessageHeaderSource(
				WithObjectStorage(lStorage),
				WithServiceRequest(req),
			),
		)

	eStorage := &testEACLStorage{
		t:      t,
		expCID: cid,
		table:  table,
	}

	validator := eacl2.NewValidator(
		eacl2.WithEACLStorage(eStorage),
	)

	require.Equal(t, eacl.ActionDeny, validator.CalculateAction(unit))

	meta.SetXHeaders(nil)

	require.Equal(t, eacl.ActionAllow, validator.CalculateAction(unit))

	meta.SetXHeaders(xHdrs)

	obj.SetAttributes(nil)

	require.Equal(t, eacl.ActionAllow, validator.CalculateAction(unit))
}
