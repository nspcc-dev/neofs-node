package v2

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

type testLocalStorage struct {
	t *testing.T

	expAddr oid.Address

	obj *object.Object

	err error
}

type testHeaderSource struct {
	header *object.Object
	err    error
	calls  []oid.Address
}

func (t *testHeaderSource) Head(_ context.Context, addr oid.Address) (*object.Object, error) {
	t.calls = append(t.calls, addr)
	return t.header, t.err
}

func (s *testLocalStorage) Head(_ context.Context, addr oid.Address) (*object.Object, error) {
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

	tgt := eacl.NewTargetByAccounts([]user.ID{userID})

	r := eacl.ConstructRecord(
		eacl.ActionDeny,
		eacl.OperationHead,
		[]eacl.Target{tgt},
		eacl.NewObjectPropertyFilter(attrKey, eacl.MatchStringEqual, attrVal),
		eacl.NewRequestHeaderFilter(xKey, eacl.MatchStringEqual, xVal))
	table := eacl.ConstructTable([]eacl.Record{r})

	lStorage := &testLocalStorage{
		t:       t,
		expAddr: oid.NewAddress(cnr, id),
		obj:     obj,
	}

	newSource := func(t *testing.T) eacl.TypedHeaderSource {
		hdrSrc, err := NewMessageHeaderSource(
			WithObjectStorage(lStorage),
			WithServiceRequest(req),
			WithCID(cnr),
			WithOID(id))
		require.NoError(t, err)
		return hdrSrc
	}

	unit := new(eacl.ValidationUnit).
		WithContainerID(&cnr).
		WithOperation(eacl.OperationHead).
		WithAccount(userID).
		WithEACLTable(&table)

	validator := eacl.NewValidator()

	checkAction(t, eacl.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))

	req.MetaHeader.XHeaders = nil

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	req.MetaHeader.XHeaders = xHdrs

	obj.SetAttributes()

	checkDefaultAction(t, validator, unit.WithHeaderSource(newSource(t)))

	lStorage.err = errors.New("any error")

	checkNonFinalAction(t, validator, unit.WithHeaderSource(newSource(t)))

	lStorage.err = nil

	r.SetAction(eacl.ActionAllow)

	rID := eacl.ConstructRecord(
		eacl.ActionDeny,
		eacl.OperationHead,
		[]eacl.Target{tgt},
		eacl.NewFilterObjectWithID(id),
	)

	table = eacl.ConstructTable([]eacl.Record{r, rID})

	unit.WithEACLTable(&table)
	checkAction(t, eacl.ActionDeny, validator, unit.WithHeaderSource(newSource(t)))
}

func checkAction(t *testing.T, expected eacl.Action, v *eacl.Validator, u *eacl.ValidationUnit) {
	actual, final, err := v.CalculateAction(u)
	require.NoError(t, err)
	require.True(t, final)
	require.Equal(t, expected, actual)
}

func checkDefaultAction(t *testing.T, v *eacl.Validator, u *eacl.ValidationUnit) {
	actual, final, err := v.CalculateAction(u)
	require.NoError(t, err)
	require.True(t, final)
	require.Equal(t, eacl.ActionAllow, actual)
}

func checkNonFinalAction(t *testing.T, v *eacl.Validator, u *eacl.ValidationUnit) {
	actual, final, err := v.CalculateAction(u)
	require.NoError(t, err)
	require.False(t, final)
	require.Equal(t, eacl.ActionAllow, actual)
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

	priv, err := keys.NewPrivateKey()
	require.NoError(t, err)
	senderKey := priv.PublicKey()
	userID := user.NewFromECDSAPublicKey((ecdsa.PublicKey)(*senderKey))

	tgt := eacl.NewTargetByAccounts([]user.ID{userID})
	r := eacl.ConstructRecord(
		eacl.ActionDeny,
		eacl.OperationPut,
		[]eacl.Target{tgt},
		eacl.NewObjectPropertyFilter(attrKey, eacl.MatchStringEqual, attrVal),
	)

	table := eacl.ConstructTable([]eacl.Record{r})

	cnr, id := cidtest.ID(), oidtest.ID()

	newUnit := func(t *testing.T, split *protoobject.Header_Split, src HeaderSource, tbl *eacl.Table) *eacl.ValidationUnit {
		t.Helper()
		req := &protoobject.PutRequest{
			Body: &protoobject.PutRequest_Body{
				ObjectPart: &protoobject.PutRequest_Body_Init_{Init: &protoobject.PutRequest_Body_Init{
					Header: &protoobject.Header{Split: split},
				}},
			},
		}
		hdrSrc, err := NewMessageHeaderSource(
			WithHeaderSource(src),
			WithServiceRequest(req),
			WithCID(cnr),
			WithOID(id),
		)
		require.NoError(t, err)
		return new(eacl.ValidationUnit).
			WithOperation(eacl.OperationPut).
			WithEACLTable(tbl).
			WithAccount(userID).
			WithHeaderSource(hdrSrc)
	}

	validator := eacl.NewValidator()

	t.Run("denied by parent's attribute; first object", func(t *testing.T) {
		// ensure fetching the first object is not possible, only already attached information
		// is available
		split := &protoobject.Header_Split{
			ParentHeader: originalObject.ProtoMessage().Header,
			First:        firstObject.GetID().ProtoMessage(),
		}

		checkAction(t, eacl.ActionDeny, validator, newUnit(t, split, &testHeaderSource{}, &table))
	})

	t.Run("denied by parent's attribute; non-first object", func(t *testing.T) {
		// get the first object from the "network"
		split := &protoobject.Header_Split{
			First: firstObject.GetID().ProtoMessage(),
		}

		checkAction(t, eacl.ActionDeny, validator, newUnit(t, split, &testHeaderSource{header: &firstObject}, &table))
	})

	t.Run("allow cause no restricted attribute found", func(t *testing.T) {
		originalObjectNoRestrictedAttr := objecttest.Object()
		originalObjectNoRestrictedAttr.SetID(oid.ID{}) // no object ID for an original object in the first object
		originalObjectNoRestrictedAttr.SetSignature(&neofscrypto.Signature{})

		split := &protoobject.Header_Split{
			ParentHeader: originalObjectNoRestrictedAttr.ProtoMessage().Header,
			First:        firstObject.GetID().ProtoMessage(),
		}

		checkDefaultAction(t, validator, newUnit(t, split, &testHeaderSource{}, &table))
	})

	t.Run("missing first object ID", func(t *testing.T) {
		action, _, err := validator.CalculateAction(newUnit(t, &protoobject.Header_Split{}, &testHeaderSource{}, &table))
		require.ErrorContains(t, err, "neither a parent header nor a first object ID")
		require.Equal(t, eacl.ActionDeny, action)
	})

	t.Run("parent header not required by policy", func(t *testing.T) {
		tbl := eacl.ConstructTable([]eacl.Record{eacl.ConstructRecord(eacl.ActionAllow, eacl.OperationPut, []eacl.Target{tgt})})
		checkAction(t, eacl.ActionAllow, validator, newUnit(t, &protoobject.Header_Split{}, &testHeaderSource{}, &tbl))
	})

	t.Run("address filters do not require parent header", func(t *testing.T) {
		tbl := eacl.ConstructTable([]eacl.Record{
			eacl.ConstructRecord(eacl.ActionAllow, eacl.OperationPut, []eacl.Target{tgt},
				eacl.NewFilterObjectsFromContainer(cnr), eacl.NewFilterObjectWithID(id)),
			eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationPut, []eacl.Target{tgt}),
		})
		checkAction(t, eacl.ActionAllow, validator, newUnit(t, &protoobject.Header_Split{}, &testHeaderSource{}, &tbl))
	})

	t.Run("address and attribute filters require parent header", func(t *testing.T) {
		tbl := eacl.ConstructTable([]eacl.Record{
			eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationPut, []eacl.Target{tgt},
				eacl.NewFilterObjectsFromContainer(cnr),
				eacl.NewFilterObjectWithID(id),
				eacl.NewObjectPropertyFilter(attrKey, eacl.MatchStringEqual, attrVal)),
		})
		action, _, err := validator.CalculateAction(newUnit(t, &protoobject.Header_Split{}, &testHeaderSource{}, &tbl))
		require.ErrorContains(t, err, "neither a parent header nor a first object ID")
		require.Equal(t, eacl.ActionDeny, action)
	})
}

func TestV2SplitMiddlePart(t *testing.T) {
	cnr := cidtest.ID()

	var parent object.Object
	parent.SetContainerID(cnr)
	parent.SetAttributes(object.NewAttribute("classified", "secret"))

	var first object.Object
	first.SetContainerID(cnr)
	first.SetParent(&parent)
	require.NoError(t, first.CalculateAndSetID())
	firstID := first.GetID()

	var middle object.Object
	middle.SetContainerID(cnr)
	middle.SetOwner(usertest.ID())
	middle.SetFirstID(firstID)
	middle.SetPreviousID(firstID)
	require.NoError(t, middle.CalculateAndSetID())

	var ecPart object.Object
	ecPart.SetContainerID(cnr)
	ecPart.SetParent(&middle)
	require.NoError(t, ecPart.CalculateAndSetID())

	newTable := func(op eacl.Operation, filter eacl.Filter) *eacl.Table {
		table := eacl.ConstructTable([]eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, op,
			[]eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}, filter)})
		return &table
	}
	secretFilter := eacl.NewObjectPropertyFilter("classified", eacl.MatchStringEqual, "secret")

	sources := func(t *testing.T, part object.Object) []struct {
		name string
		op   eacl.Operation
		opts []Option
	} {
		addr := oid.NewAddress(cnr, part.GetID()).ProtoMessage()
		getReq := &protoobject.GetRequest{Body: &protoobject.GetRequest_Body{Address: addr}}
		headReq := &protoobject.HeadRequest{Body: &protoobject.HeadRequest_Body{Address: addr}}
		storage := &testLocalStorage{t: t, expAddr: oid.NewAddress(cnr, part.GetID()), obj: &part}
		hdr := part.ProtoMessage().Header
		return []struct {
			name string
			op   eacl.Operation
			opts []Option
		}{
			{"GET request", eacl.OperationGet, []Option{WithObjectStorage(storage), WithServiceRequest(getReq)}},
			{"HEAD request", eacl.OperationHead, []Option{WithObjectStorage(storage), WithServiceRequest(headReq)}},
			{"decoded header", eacl.OperationGet, []Option{WithObjectHeader(&part, getReq)}},
			{"binary header", eacl.OperationGet, []Option{WithObjectHeaderBinary(marshalObjectHeader(hdr))}},
			{"unordered binary header", eacl.OperationGet, []Option{WithObjectHeaderBinary(swapFirstFields(t, marshalObjectHeader(hdr)))}},
			{"unordered binary split header", eacl.OperationGet, []Option{WithObjectHeaderBinary(swapSplitFields(t, marshalObjectHeader(hdr)))}},
			{"GET response", eacl.OperationGet, []Option{WithServiceResponse(&protoobject.GetResponse{Body: &protoobject.GetResponse_Body{
				ObjectPart: &protoobject.GetResponse_Body_Init_{Init: &protoobject.GetResponse_Body_Init{ObjectId: part.GetID().ProtoMessage(), Header: hdr}},
			}}, getReq)}},
			{"HEAD response", eacl.OperationHead, []Option{WithServiceResponse(&protoobject.HeadResponse{Body: &protoobject.HeadResponse_Body{
				Head: &protoobject.HeadResponse_Body_Header{Header: &protoobject.HeaderWithSignature{Header: hdr}},
			}}, headReq)}},
		}
	}

	calculate := func(t *testing.T, hs *testHeaderSource, part object.Object, op eacl.Operation, filter eacl.Filter, opts []Option) (eacl.Action, bool, error) {
		src, err := NewMessageHeaderSource(append([]Option{WithCID(cnr), WithOID(part.GetID()), WithHeaderSource(hs)}, opts...)...)
		require.NoError(t, err)
		unit := new(eacl.ValidationUnit).WithContainerID(&cnr).WithRole(eacl.RoleOthers).WithOperation(op).
			WithEACLTable(newTable(op, filter)).WithHeaderSource(src)
		return eacl.NewValidator().CalculateAction(unit)
	}

	for _, part := range []struct {
		name string
		obj  object.Object
	}{{"middle part", middle}, {"EC part of middle part", ecPart}} {
		t.Run(part.name, func(t *testing.T) {
			for _, src := range sources(t, part.obj) {
				t.Run(src.name, func(t *testing.T) {
					t.Run("denied by parent attribute", func(t *testing.T) {
						hs := &testHeaderSource{header: &first}
						action, final, err := calculate(t, hs, part.obj, src.op, secretFilter, src.opts)
						require.NoError(t, err)
						require.True(t, final)
						require.Equal(t, eacl.ActionDeny, action)
						require.Equal(t, []oid.Address{oid.NewAddress(cnr, firstID)}, hs.calls)
					})
					t.Run("first part unavailable", func(t *testing.T) {
						hs := &testHeaderSource{err: errors.New("any error")}
						_, _, err := calculate(t, hs, part.obj, src.op, secretFilter, src.opts)
						require.ErrorIs(t, err, hs.err)
						require.NotErrorIs(t, err, eacl.ErrHeaderUnavailable)
					})
					t.Run("first part without parent", func(t *testing.T) {
						hs := &testHeaderSource{header: &middle}
						_, _, err := calculate(t, hs, part.obj, src.op, secretFilter, src.opts)
						require.Error(t, err)
						require.NotErrorIs(t, err, eacl.ErrHeaderUnavailable)
					})
					t.Run("address filter", func(t *testing.T) {
						hs := new(testHeaderSource)
						_, _, err := calculate(t, hs, part.obj, src.op, eacl.NewFilterObjectWithID(firstID), src.opts)
						require.NoError(t, err)
						require.Empty(t, hs.calls)
					})
				})
			}
		})
	}

	t.Run("V1 split", func(t *testing.T) {
		v1 := middle
		v1.SetFirstID(oid.ID{})
		v1.SetSplitID(object.NewSplitID())
		hs := new(testHeaderSource)
		for _, src := range sources(t, v1) {
			t.Run(src.name, func(t *testing.T) {
				action, final, err := calculate(t, hs, v1, src.op, secretFilter, src.opts)
				require.NoError(t, err)
				require.True(t, final)
				require.Equal(t, eacl.ActionAllow, action)
				require.Empty(t, hs.calls)
			})
		}
	})

	t.Run("PUT", func(t *testing.T) {
		for _, part := range []struct {
			name string
			obj  object.Object
		}{{"middle part", middle}, {"EC part of middle part", ecPart}} {
			t.Run(part.name, func(t *testing.T) {
				req := &protoobject.PutRequest{Body: &protoobject.PutRequest_Body{
					ObjectPart: &protoobject.PutRequest_Body_Init_{Init: &protoobject.PutRequest_Body_Init{Header: part.obj.ProtoMessage().Header}},
				}}
				opts := []Option{WithServiceRequest(req)}
				t.Run("denied by parent attribute", func(t *testing.T) {
					hs := &testHeaderSource{header: &first}
					action, final, err := calculate(t, hs, part.obj, eacl.OperationPut, secretFilter, opts)
					require.NoError(t, err)
					require.True(t, final)
					require.Equal(t, eacl.ActionDeny, action)
					require.Equal(t, []oid.Address{oid.NewAddress(cnr, firstID)}, hs.calls)
				})
				t.Run("first part without parent", func(t *testing.T) {
					action, _, err := calculate(t, &testHeaderSource{header: &middle}, part.obj, eacl.OperationPut, secretFilter, opts)
					require.ErrorContains(t, err, "has no parent header")
					require.Equal(t, eacl.ActionDeny, action)
				})
				t.Run("first part unavailable", func(t *testing.T) {
					hs := &testHeaderSource{err: errors.New("any error")}
					action, _, err := calculate(t, hs, part.obj, eacl.OperationPut, secretFilter, opts)
					require.ErrorIs(t, err, hs.err)
					require.Equal(t, eacl.ActionDeny, action)
				})
			})
		}
	})
}

func swapFirstFields(t *testing.T, msg []byte) []byte {
	_, _, n1 := protowire.ConsumeField(msg)
	require.Positive(t, n1)
	_, _, n2 := protowire.ConsumeField(msg[n1:])
	require.Positive(t, n2)
	return slices.Concat(msg[n1:n1+n2], msg[:n1], msg[n1+n2:])
}

func swapSplitFields(t *testing.T, hdr []byte) []byte {
	f, err := iprotobuf.GetLENFieldBounds(hdr, protoobject.FieldHeaderSplit)
	require.NoError(t, err)
	require.False(t, f.IsMissing())
	return slices.Concat(hdr[:f.ValueFrom], swapFirstFields(t, hdr[f.ValueFrom:f.To]), hdr[f.To:])
}

func marshalObjectHeader(hdr *protoobject.Header) []byte {
	buf := make([]byte, hdr.MarshaledSize())
	hdr.MarshalStable(buf)
	return buf
}

func objectHeaderOptions(hdr *protoobject.Header) []struct {
	name string
	opt  Option
} {
	return []struct {
		name string
		opt  Option
	}{
		{"binary", WithObjectHeaderBinary(marshalObjectHeader(hdr))},
		{"decoded", WithServiceResponse(&protoobject.HeadResponse{Body: &protoobject.HeadResponse_Body{
			Head: &protoobject.HeadResponse_Body_Header{Header: &protoobject.HeaderWithSignature{Header: hdr}},
		}}, nil)},
	}
}

func TestExpirationTimeRule(t *testing.T) {
	for _, tc := range []struct {
		name       string
		expiration time.Time
		action     eacl.Action
	}{
		{name: "absent", action: eacl.ActionDeny},
		{name: "minimum timestamp", expiration: time.Unix(1, 0), action: eacl.ActionDeny},
		{name: "below threshold", expiration: time.Unix(1749999999, 0), action: eacl.ActionDeny},
		{name: "equal to threshold", expiration: time.Unix(1750000000, 0), action: eacl.ActionAllow},
		{name: "above threshold", expiration: time.Unix(1750000001, 0), action: eacl.ActionAllow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var obj object.Object
			obj.SetCreationEpoch(1)
			if !tc.expiration.IsZero() {
				require.NoError(t, obj.SetExpirationTime(tc.expiration))
			}
			for _, source := range objectHeaderOptions(obj.ProtoMessage().Header) {
				t.Run(source.name, func(t *testing.T) {
					src, err := NewMessageHeaderSource(source.opt)
					require.NoError(t, err)
					targets := []eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}
					table := eacl.ConstructTable([]eacl.Record{
						eacl.ConstructRecord(eacl.ActionAllow, eacl.OperationHead, targets,
							eacl.NewObjectPropertyFilter("$Object:expirationTime", eacl.MatchNumGE, "1750000000")),
						eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationHead, targets),
					})
					unit := new(eacl.ValidationUnit).
						WithRole(eacl.RoleOthers).
						WithOperation(eacl.OperationHead).
						WithEACLTable(&table).
						WithHeaderSource(src)
					checkAction(t, tc.action, eacl.NewValidator(), unit)
				})
			}
		})
	}
}

type countingRequest struct {
	metaCalls int
}

func (r *countingRequest) GetMetaHeader() *protosession.RequestMetaHeader {
	r.metaCalls++
	return &protosession.RequestMetaHeader{XHeaders: []*protosession.XHeader{{Key: "empty"}}}
}

func TestReservedObjectHeaderLookup(t *testing.T) {
	for _, withParent := range []bool{false, true} {
		name := "object"
		if withParent {
			name = "parent"
		}
		t.Run(name, func(t *testing.T) {
			attrs := []object.Attribute{
				object.NewAttribute(eacl.FilterObjectCreationEpoch, "999"),
				object.NewAttribute(object.FilterExpirationTime, "999"),
				object.NewAttribute("$Object:unknown", "forged"),
				object.NewAttribute("ordinary", "found"),
			}
			var obj object.Object
			obj.SetCreationEpoch(1)
			if withParent {
				var parent object.Object
				parent.SetCreationEpoch(1)
				parent.SetAttributes(attrs...)
				obj.SetParent(&parent)
			} else {
				obj.SetAttributes(attrs...)
			}
			for _, source := range objectHeaderOptions(obj.ProtoMessage().Header) {
				src, err := NewMessageHeaderSource(source.opt)
				require.NoError(t, err)
				for _, kv := range []struct {
					key   string
					value []byte
				}{
					{eacl.FilterObjectCreationEpoch, []byte("1")},
					{object.FilterExpirationTime, nil},
					{"$Object:unknown", nil},
					{"ordinary", []byte("found")},
				} {
					value, err := src.HeaderValue(eacl.HeaderFromObject, kv.key)
					require.NoError(t, err)
					require.Equal(t, kv.value, value, kv.key)
				}
			}
		})
	}
}

func TestObjectHeaders(t *testing.T) {
	t.Run("request headers are read only on request lookup", func(t *testing.T) {
		req := new(countingRequest)
		src, err := NewMessageHeaderSource(WithServiceResponse(new(protoobject.HeadResponse), req))
		require.NoError(t, err)
		_, err = src.HeaderValue(eacl.HeaderFromObject, eacl.FilterObjectContainerID)
		require.NoError(t, err)
		require.Zero(t, req.metaCalls)

		value, err := src.HeaderValue(eacl.HeaderFromRequest, "empty")
		require.NoError(t, err)
		require.Equal(t, []byte{}, value)
		require.Equal(t, 1, req.metaCalls)
	})

	t.Run("binary rejects trailing data after match", func(t *testing.T) {
		var obj object.Object
		obj.SetAttributes(object.NewAttribute("key", "value"))
		buf := marshalObjectHeader(obj.ProtoMessage().Header)
		src, err := NewMessageHeaderSource(WithObjectHeaderBinary(append(buf, 0x80))) // truncated tag
		require.NoError(t, err)
		for range 2 {
			value, err := src.HeaderValue(eacl.HeaderFromObject, "key")
			require.Error(t, err)
			require.Nil(t, value)
		}
	})

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
	//nolint:staticcheck // this is a test and such objects are possible
	homoHash := checksum.New(checksum.TillichZemor, []byte{199, 94, 209, 171, 81, 255, 104, 105, 199, 78, 103, 145, 33, 82, 51, 104, 36, 71, 94, 186, 122, 163, 215, 213, 37, 126, 202, 109, 135, 119, 180, 166, 97, 152, 172,
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

	testWithOption := func(t *testing.T, id oid.ID, exp [][2]string, opt Option) {
		src, err := NewMessageHeaderSource(
			WithCID(cnr),
			WithOID(id),
			opt,
		)
		require.NoError(t, err)

		filters := make([]eacl.Filter, 0, len(exp))
		for _, kv := range exp {
			filters = append(filters, eacl.NewObjectPropertyFilter(kv[0], eacl.MatchStringEqual, kv[1]))
		}
		table := eacl.ConstructTable([]eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationHead,
			[]eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}, filters...)})
		unit := new(eacl.ValidationUnit).WithRole(eacl.RoleOthers).WithOperation(eacl.OperationHead).WithEACLTable(&table).WithHeaderSource(src)
		checkAction(t, eacl.ActionDeny, eacl.NewValidator(), unit)

		for _, kv := range exp {
			value, err := src.HeaderValue(eacl.HeaderFromObject, kv[0])
			require.NoError(t, err)
			require.Equal(t, []byte(kv[1]), value, kv[0])
		}
		value, err := src.HeaderValue(eacl.HeaderFromObject, "missing")
		require.NoError(t, err)
		require.Nil(t, value)
		if id.IsZero() {
			value, err := src.HeaderValue(eacl.HeaderFromObject, eacl.FilterObjectID)
			require.NoError(t, err)
			require.Nil(t, value)
		}
	}

	testWithObject := func(t *testing.T, id oid.ID, obj object.Object, exp [][2]string) {
		t.Run("binary", func(t *testing.T) {
			testWithOption(t, id, exp, WithObjectHeaderBinary(marshalObjectHeader(obj.ProtoMessage().Header)))
		})

		testWithOption(t, id, exp, WithServiceResponse(newHeadResponse(obj), nil))
	}

	t.Run("min", func(t *testing.T) {
		testWithObject(t, oid.ID{}, obj, [][2]string{
			{"$Object:version", "v0.0"},
			{"$Object:containerID", cnrStr},
			{"$Object:creationEpoch", creationEpochStr},
			{"$Object:payloadLength", payloadLenStr},
			{"$Object:objectType", "REGULAR"},
		})
	})

	require.NoError(t, obj.SetExpirationTime(time.Unix(1<<40, 0)))
	ver := version.New(12, 34)
	obj.SetVersion(&ver)
	obj.SetOwner(owner)
	obj.SetPayloadChecksum(payloadHash)
	obj.SetPayloadHomomorphicHash(homoHash) //nolint:staticcheck // this is a test and such objects are possible
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
		//nolint:staticcheck // this is a test and such objects are possible
		childHomoHash := checksum.New(checksum.TillichZemor, []byte{191, 33, 176, 230, 72, 226, 40, 54, 248, 99, 98, 74, 97, 223, 128, 145, 184, 14, 124, 85, 113, 204, 145, 55, 214, 96, 210, 161, 27, 170, 203, 191,
			246, 83, 173, 163, 112, 152, 185, 131, 251, 218, 64, 145, 78, 29, 203, 105, 12, 150, 123, 54, 109, 23, 29, 140, 87, 196, 70, 81, 88, 110, 152, 99})
		const childHomoHashStr = "TZ:bf21b0e648e22836f863624a61df8091b80e7c5571cc9137d660d2a11baacbbff653ada37098b983fbda40914e1dcb690c967b366d171d8c57c44651586e9863"

		var child object.Object
		child.SetContainerID(cidtest.OtherID(cnr))
		child.SetID(oidtest.OtherID(id))
		child.SetOwner(childOwner)
		child.SetPayloadSize(childPayloadLen)
		child.SetCreationEpoch(childCreationEpoch)
		require.NoError(t, child.SetExpirationTime(time.Unix(1<<40+1, 0)))
		ver := version.New(56, 78)
		child.SetVersion(&ver)
		child.SetType(object.TypeLock)
		child.SetPayloadChecksum(childPayloadHash)
		child.SetAttributes(object.NewAttribute("foo", "child"))
		child.SetParent(&obj)

		checkParentHashRule := func(t *testing.T, want eacl.Action) {
			table := eacl.ConstructTable([]eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationHead,
				[]eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)},
				eacl.NewObjectPropertyFilter("$Object:homomorphicHash", eacl.MatchStringEqual, homoHashStr))})
			for _, source := range objectHeaderOptions(child.ProtoMessage().Header) {
				t.Run(source.name, func(t *testing.T) {
					src, err := NewMessageHeaderSource(WithCID(cnr), WithOID(id), source.opt)
					require.NoError(t, err)
					unit := new(eacl.ValidationUnit).WithRole(eacl.RoleOthers).WithOperation(eacl.OperationHead).WithEACLTable(&table).WithHeaderSource(src)
					checkAction(t, want, eacl.NewValidator(), unit)
				})
			}
		}
		t.Run("parent hash is used when child has none", func(t *testing.T) {
			checkParentHashRule(t, eacl.ActionDeny)
		})
		child.SetPayloadHomomorphicHash(childHomoHash) //nolint:staticcheck // this is a test and such objects are possible
		t.Run("child hash shadows parent hash", func(t *testing.T) {
			checkParentHashRule(t, eacl.ActionAllow)
		})

		testWithObject(t, id, child, [][2]string{
			{"$Object:objectID", idStr},
			{"$Object:version", "v56.78"},
			{"$Object:containerID", cnrStr},
			{"$Object:ownerID", childOwnerStr},
			{"$Object:creationEpoch", childCreationEpochStr},
			{"$Object:expirationTime", "1099511627777"},
			{"$Object:payloadLength", childPayloadLenStr},
			{"$Object:objectType", "LOCK"},
			{"$Object:payloadHash", childPayloadHashStr},
			{"$Object:homomorphicHash", childHomoHashStr},
			{"foo", "child"},
			{"hello", "world"},
		})
	})

	testWithObject(t, id, obj, [][2]string{
		{"$Object:objectID", idStr},
		{"$Object:version", "v12.34"},
		{"$Object:containerID", cnrStr},
		{"$Object:ownerID", ownerStr},
		{"$Object:creationEpoch", creationEpochStr},
		{"$Object:expirationTime", "1099511627776"},
		{"$Object:payloadLength", payloadLenStr},
		{"$Object:objectType", "REGULAR"},
		{"$Object:payloadHash", payloadHashStr},
		{"$Object:homomorphicHash", homoHashStr},
		{"foo", "bar"},
		{"hello", "world"},
	})
}

// BenchmarkHeaderFilters uses the validator API shared by both versions of the
// header source, so the same benchmark can be run on the baseline revision.
func BenchmarkHeaderFilters(b *testing.B) {
	cnr, id := cidtest.ID(), oidtest.ID()
	value := strings.Repeat("a", 64)
	for _, count := range []int{8, 128} {
		attrs := make([]object.Attribute, count)
		for i := range attrs {
			attrs[i] = object.NewAttribute(fmt.Sprintf("attr-%03d", i), value)
		}
		var obj object.Object
		obj.SetCreationEpoch(123456789)
		obj.SetAttributes(attrs...)
		buf := marshalObjectHeader(obj.ProtoMessage().Header)
		for _, src := range []struct {
			name string
			opt  Option
		}{
			{"binary", WithObjectHeaderBinary(buf)},
			{"decoded", WithServiceRequest(new(protoobject.HeadRequest))},
		} {
			benchmark := func(b *testing.B, records []eacl.Record) {
				table := eacl.ConstructTable(records)
				unit := new(eacl.ValidationUnit).WithRole(eacl.RoleOthers).WithOperation(eacl.OperationHead).WithEACLTable(&table)
				v := eacl.NewValidator()
				opts := []Option{WithCID(cnr), WithOID(id), WithObjectStorage(&testHeaderSource{header: &obj}), src.opt}
				h, err := NewMessageHeaderSource(opts...)
				require.NoError(b, err)
				action, final, err := v.CalculateAction(unit.WithHeaderSource(h))
				require.NoError(b, err)
				require.True(b, final)
				require.Equal(b, eacl.ActionDeny, action)

				b.ReportAllocs()
				for b.Loop() {
					h, err := NewMessageHeaderSource(opts...)
					if err != nil {
						b.Fatal(err)
					}
					if action, final, err := v.CalculateAction(unit.WithHeaderSource(h)); err != nil || !final || action != eacl.ActionDeny {
						b.Fatalf("unexpected evaluation: %v, %v, %v", action, final, err)
					}
				}
			}
			targets := []eacl.Target{eacl.NewTargetByRole(eacl.RoleOthers)}
			for _, n := range []int{1, 4} {
				b.Run(fmt.Sprintf("%s/attrs=%d/filters=%d", src.name, count, n), func(b *testing.B) {
					filters := make([]eacl.Filter, n)
					// Look near the end to include the cost of scanning attributes.
					for i := range filters {
						filters[i] = eacl.NewObjectPropertyFilter(attrs[count-1-i].Key(), eacl.MatchStringEqual, value)
					}
					benchmark(b, []eacl.Record{eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationHead, targets, filters...)})
				})
				b.Run(fmt.Sprintf("%s/attrs=%d/system=creationEpoch/records=%d", src.name, count, n), func(b *testing.B) {
					records := make([]eacl.Record, n)
					// Only the last record matches, forcing repeated lookups of the same key.
					for i := range records {
						filter := eacl.NewFilterObjectCreationEpochIs(eacl.MatchStringEqual, 123456789+uint64(n-1-i))
						records[i] = eacl.ConstructRecord(eacl.ActionDeny, eacl.OperationHead, targets, filter)
					}
					benchmark(b, records)
				})
			}
		}
	}
}
