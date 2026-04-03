package object_test

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
)

func newBlankMetaHeader() *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: &protorefs.Version{
			Major: 3651676384,
			Minor: 2829345803,
		},
		Epoch: 10699904184716558895,
		Ttl:   1657590594,
		XHeaders: []*protosession.XHeader{
			{Key: "key1", Value: ""},
			{Key: "", Value: "value2"},
			{Key: "key3", Value: "value3"},
			{Key: "", Value: ""},
		},
	}
}

func nestMetaHeader(m *protosession.ResponseMetaHeader, n int) *protosession.ResponseMetaHeader {
	for range n {
		st := m.GetStatus()
		m = &protosession.ResponseMetaHeader{
			Version:  m.Version,
			Epoch:    m.Epoch,
			Ttl:      m.Ttl,
			XHeaders: m.XHeaders,
			Origin:   m,
			Status: &protostatus.Status{
				Code:    st.GetCode() + 1,
				Message: st.GetMessage(),
				Details: st.GetDetails(),
			},
		}
	}
	return m
}

func newTestSplitInfo() *protoobject.SplitInfo {
	var res object.SplitInfo
	res.SetSplitID(object.NewSplitID())
	res.SetLastPart(oidtest.ID())
	res.SetLink(oidtest.ID())
	res.SetFirstPart(oidtest.ID())
	return res.ProtoMessage()
}

func newAnyVerificationHeader() *protosession.ResponseVerificationHeader {
	return &protosession.ResponseVerificationHeader{
		BodySignature: &protorefs.Signature{
			Key:    []byte("any_body_key"),
			Sign:   []byte("any_body_signature"),
			Scheme: 123,
		},
		MetaSignature: &protorefs.Signature{
			Key:    []byte("any_meta_key"),
			Sign:   []byte("any_meta_signature"),
			Scheme: 456,
		},
		OriginSignature: &protorefs.Signature{
			Key:    []byte("any_origin_key"),
			Sign:   []byte("any_origin_signature"),
			Scheme: 789,
		},
		Origin: &protosession.ResponseVerificationHeader{
			BodySignature: &protorefs.Signature{
				Key:    []byte("any_origin_body_key"),
				Sign:   []byte("any_origin_body_signature"),
				Scheme: 321,
			},
			MetaSignature: &protorefs.Signature{
				Key:    []byte("any_origin_meta_key"),
				Sign:   []byte("any_origin_meta_signature"),
				Scheme: 654,
			},
			OriginSignature: &protorefs.Signature{
				Key:    []byte("any_origin_origin_key"),
				Sign:   []byte("any_origin_origin_signature"),
				Scheme: 987,
			},
		},
	}
}
